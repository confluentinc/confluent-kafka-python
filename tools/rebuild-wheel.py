from wheel.wheelfile import WheelFile
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rebuilds wheel files.')
    parser.add_argument(
        '--staging-dir',
        required=True,
        help='Directory with all the files staged for packaging.',
    )
    parser.add_argument(
        '--wheel-file',
        required=True,
        help='The target wheel file (will be overwritten).',
    )
    args = parser.parse_args()

    wheel_file = WheelFile(args.wheel_file, mode='w')
    try:
        wheel_file.write_files(args.staging_dir)
    finally:
        wheel_file.close()
