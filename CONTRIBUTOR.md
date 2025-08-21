# Contributing to confluent-kafka-python

Thank you for your interest in contributing to confluent-kafka-python! This document provides guidelines and best practices for contributing to this project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Code Style and Standards](#code-style-and-standards)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)
- [Community Guidelines](#community-guidelines)

## Getting Started

### Ways to Contribute

- **Bug Reports**: Report bugs and issues you encounter
- **Feature Requests**: Suggest new features or improvements
- **Code Contributions**: Fix bugs, implement features, or improve documentation
- **Documentation**: Improve existing docs or add new documentation
- **Testing**: Help improve test coverage and quality

### Before You Start

1. Check existing [issues](../../issues) to see if your bug/feature has already been reported
2. For major changes, open an issue first to discuss the proposed changes
3. Fork the repository and create a feature branch for your work

## Development Environment Setup

For complete development environment setup instructions, including prerequisites, virtual environment creation, and dependency installation, see the [Development Environment Setup section in DEVELOPER.md](DEVELOPER.md#development-environment-setup).

## Code Style and Standards

### Python Code Style

- **PEP 8**: Follow [PEP 8](https://pep8.org/) style guidelines as a default, with exceptions captured in the `tox.ini` flake8 rules for modern updates to the recommendations
- **Docstrings**: Use Google-style docstrings for all public functions and classes

### Code Formatting

We use automated tools to maintain consistent code style:

```bash
# Install formatting tools
pip install flake8

# Check style
flake8 src/ tests/
```

### Naming Conventions

- **Functions and Variables**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private Methods/Objects**: Prefix with single underscore `_private_method`

### Documentation

- All public APIs must have docstrings
- Include examples in docstrings when helpful
- Keep docstrings concise but complete
- Update relevant documentation files when making changes

## Testing

### Running Tests

See [tests/README.md](tests/README.md) for comprehensive testing instructions.

### Test Requirements

- **Unit Tests**: All new functionality must include unit tests
- **Integration Tests**: Add integration tests for complex features
- **Test Coverage**: Maintain or improve existing test coverage
- **Test Naming**: Use descriptive test names that explain what is being tested

### Test Structure

```python
def test_feature_should_behave_correctly_when_condition():
    # Arrange
    setup_data = create_test_data()
    
    # Act
    result = function_under_test(setup_data)
    
    # Assert
    assert result.expected_property == expected_value
```

## Submitting Changes

### Pull Request Process

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-number-description
   ```

2. **Make Your Changes**
   - Write clean, well-documented code
   - Add appropriate tests
   - Update documentation if needed
   - Add an entry to the CHANGELOG.md file for the proposed change

3. **Test Your Changes**
   Refer to [tests/README.md](tests/README.md) 

4. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "Clear, descriptive commit message"
   ```

   **Commit Message Guidelines:**
   - Use present tense ("Add feature" not "Added feature")
   - Keep first line under 50 characters
   - Reference issue numbers when applicable (#123)
   - Include breaking change notes if applicable

5. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```
   
   Then create a pull request through GitHub's interface.

### Pull Request Guidelines

- **Title**: Clear and descriptive
- **Description**: Explain what changes you made and why
- **Linked Issues**: Reference related issues using "Fixes #123" or "Relates to #123"
- **Labels**: Review available issue/PR labels and apply relevant ones to help with categorization and triage
- **Documentation**: Update documentation for user-facing changes
- **Tests**: Include appropriate tests
- **Breaking Changes**: Clearly mark any breaking changes

### Code Review Process

- All pull requests require review before merging
- Address reviewer feedback promptly
- Keep discussions respectful and constructive
- Be open to suggestions and alternative approaches

## Reporting Issues

### Using Labels

When creating issues or pull requests, please review the available labels and apply those that are relevant to your submission. This helps maintainers categorize and prioritize work effectively. Common label categories include (look at available labels / other issues for options):

- **Type**: bug, enhancement, documentation, question
- **Priority**: high, medium, low
- **Component**: producer, consumer, admin, schema-registry, etc
- **Status**: needs-investigation, help-wanted, good-first-issue, etc

### Bug Reports

When reporting bugs, please include:

- **Clear Title**: Describe the issue concisely
- **Environment**: Python version, OS, library versions
- **Steps to Reproduce**: Detailed steps to reproduce the issue
- **Expected Behavior**: What you expected to happen
- **Actual Behavior**: What actually happened
- **Code Sample**: Minimal code that demonstrates the issue
- **Error Messages**: Full error messages and stack traces
- **Client Configuration**: Specify how the client was configured and setup
- **Logs**: Client logs when possible
- **Labels**: Apply relevant labels such as "bug" and component-specific labels

### Feature Requests

For feature requests, please include:

- **Use Case**: Describe the problem you're trying to solve
- **Proposed Solution**: Your idea for how to address it
- **Alternatives**: Other solutions you've considered
- **Additional Context**: Any other relevant information
- **Labels**: Apply relevant labels such as "enhancement" and component-specific labels

## Community Guidelines

### Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/). By participating, you agree to uphold this code.

### Communication

- **Be Respectful**: Treat all community members with respect
- **Be Constructive**: Provide helpful feedback and suggestions
- **Be Patient**: Remember that maintainers and contributors volunteer their time
- **Be Clear**: Communicate clearly and provide sufficient context

### Getting Help

- **Issues**: Use GitHub issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and general discussion
- **Documentation**: Check existing documentation before asking questions

## Recognition

Contributors are recognized in the following ways:

- Contributors are listed in the project's contributor history
- Significant contributions may be mentioned in release notes

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).

---

Thank you for contributing to confluent-kafka-python! Your contributions help make this project better for everyone. 