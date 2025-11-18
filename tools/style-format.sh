#!/bin/bash
#
# Check or apply/fix the project coding style to all files passed as arguments.
# Uses clang-format for C, and black/isort/flake8 for Python.
#
# Requirements:
#   - clang-format version 10 or higher
#     * Linux: apt install clang-format-10
#     * macOS: brew install clang-format
#   - black and isort for Python formatting
#     * pip install black isort
#


CLANG_FORMAT=${CLANG_FORMAT:-clang-format}

set -e

ret=0

if [[ -z $1 ]]; then
    echo "Usage: $0 [--fix] srcfile1.c srcfile2.h srcfile3.c ..."
    echo ""
    exit 0
fi

if [[ $1 == "--fix" ]]; then
    fix=1
    shift
else
    fix=0
fi

# Check if we have any C/C++ files to process
has_c_files=0
for f in "$@"; do
    if [[ $f == *.c ]] || [[ $f == *.h ]]; then
        has_c_files=1
        break
    fi
done

# Only check clang-format if we have C files to process
if [[ $has_c_files -eq 1 ]]; then
    clang_format_version=$(${CLANG_FORMAT} --version 2>/dev/null | sed -Ee 's/.*version ([[:digit:]]+)\.[[:digit:]]+\.[[:digit:]]+.*/\1/' || echo "0")
    if [[ -z $clang_format_version ]] || [[ $clang_format_version == "0" ]] || [[ $clang_format_version -lt 10 ]] ; then
        echo "$0: clang-format version 10 or higher required, but version '$clang_format_version' detected (or not found)" 1>&2
        exit 1
    fi
fi

# Get list of files from .formatignore to ignore formatting for.
ignore_files=( $(grep '^[^#]..' .formatignore) )

function ignore {
    local file=$1

    local f
    for f in "${ignore_files[@]}" ; do
        [[ $file == $f ]] && return 0
    done

    return 1
}

extra_info=""

file_count=$#
file_num=0

for f in $*; do
    file_num=$((file_num + 1))
    
    if ignore $f ; then
        echo "$f is ignored by .formatignore" 1>&2
        continue
    fi

    lang="c"
    if [[ $f == *.py ]]; then
        lang="py"
        style="pep8"
        stylename="pep8"
    else
        style="file"  # Use .clang-format
        stylename="C"
    fi

    check=0

    if [[ $fix == 1 ]]; then
        # Convert tabs to 8 spaces first.
        if grep -ql $'\t' "$f"; then
            sed -i -e 's/\t/        /g' "$f"
            echo "$f: tabs converted to spaces"
        fi

        if [[ $lang == c ]]; then
            # Run clang-format to reformat the file
            ${CLANG_FORMAT} --style="$style" "$f" > _styletmp

        else
            # Run isort and black to format the file 
            # First, create a backup to compare
            cp "$f" _styletmp_orig
            # Run isort to sort imports
            python3 -m isort "$f" > /dev/null 2>&1 || true
            # Run black to format the file
            python3 -m black "$f" > /dev/null 2>&1 || true
            # Compare to see if changes were made
            if ! cmp -s "$f" _styletmp_orig; then
                echo "[$file_num/$file_count] $f: style fixed (isort + black)"
            else
                # Show progress
                echo -n "[$file_num/$file_count] Processing $f... " 1>&2
                echo "OK" 1>&2
            fi
            rm -f _styletmp_orig
            #  Run flake8 check after formatting
            check=1
        fi
    fi

    if [[ $fix == 0 || $check == 1 ]]; then
        file_has_errors=0
        
        # Show progress
        if [[ $fix == 0 ]]; then
            echo -n "[$file_num/$file_count] Checking $f... " 1>&2
        fi
        
        # Check for tabs
        if grep -q $'\t' "$f" ; then
            echo "$f: contains tabs: convert to 8 spaces instead"
            file_has_errors=1
            ret=1
        fi

        # Check style
        if [[ $lang == c ]]; then
            set +e
            clang_output=$(${CLANG_FORMAT} --style="$style" --Werror --dry-run "$f" 2>&1)
            clang_exit=$?
            set -e
            if [[ $clang_exit -ne 0 ]]; then
                # Output clang-format errors if any
                if [[ -n "$clang_output" ]]; then
                    echo "$clang_output" 1>&2
                fi
                echo "$f: had style errors ($stylename): see clang-format output above"
                file_has_errors=1
                ret=1
            fi
        elif [[ $lang == py ]]; then
            # Check with isort first
            set +e
            isort_check=$(python3 -m isort --check-only --diff "$f" 2>&1)
            isort_exit=$?
            set -e
            if [[ $isort_exit -ne 0 ]]; then
                echo "$f: import sorting issues (isort)" 1>&2
                echo "$isort_check" | head -10 1>&2
                file_has_errors=1
                ret=1
            fi
            # Check with black
            set +e
            black_check=$(python3 -m black --check --diff "$f" 2>&1)
            black_exit=$?
            set -e
            if [[ $black_exit -ne 0 ]]; then
                echo "$f: formatting issues (black)" 1>&2
                echo "$black_check" | head -10 1>&2
                file_has_errors=1
                ret=1
            fi
            # Also run flake8 for linting (not formatting)
            set +e
            flake8_output=$(python3 -m flake8 "$f" 2>&1)
            flake8_exit=$?
            set -e
            if [[ $flake8_exit -ne 0 ]]; then
                # Output flake8 errors
                if [[ -n "$flake8_output" ]]; then
                    echo "$flake8_output" 1>&2
                fi
                echo "$f: had linting errors (flake8): see flake8 output above"
                if [[ $fix == 1 ]]; then
                    extra_info="Error: black/isort could not fix all linting errors, fix the flake8 errors manually and run again."
                fi
                file_has_errors=1
                ret=1
            fi
        fi
        
        # Show OK if no errors found
        if [[ $fix == 0 ]] && [[ $file_has_errors -eq 0 ]]; then
            echo "OK" 1>&2
        fi
    fi

done

rm -f _styletmp

if [[ $ret != 0 ]]; then
    echo ""
    echo "You can run the following command to automatically fix the style:"
    echo "  $ make style-fix"
    [[ -n $extra_info ]] && echo "$extra_info"
fi

exit $ret
