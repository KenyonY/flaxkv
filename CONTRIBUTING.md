# Contributing to flaxkv

First of all, thank you for considering contributing to our project! We appreciate your time and effort, and we value any contribution, whether it's reporting a bug, suggesting a new feature, or submitting a pull request.

This document provides guidelines and best practices to help you contribute effectively.

## How to Contribute

### Reporting Bugs

If you find a bug in the project, please create an issue on GitHub with the following information:

- A clear, descriptive title for the issue.
- A description of the problem, including steps to reproduce the issue.
- Any relevant logs, screenshots, or other supporting information.

### Suggesting Enhancements

If you have an idea for a new feature or improvement, please create an issue on GitHub with the following information:

- A clear, descriptive title for the issue.
- A detailed description of the proposed enhancement, including any benefits and potential drawbacks.
- Any relevant examples, mockups, or supporting information.

### Submitting Pull Requests

When submitting a pull request, please ensure that your changes meet the following criteria:

- Your pull request should be atomic and focus on a single change.
- Your pull request should include tests for your change.
- You should have thoroughly tested your changes with multiple different prompts.
- You should have considered potential risks and mitigations for your changes.
- You should have documented your changes clearly and comprehensively.
- You should not include any unrelated or "extra" small tweaks or changes.

## Style Guidelines

### Code Formatting

We use the `black` code formatter to maintain a consistent coding style across the project. Please ensure that your code is formatted using `black` before submitting a pull request. You can install `black` using `pip`:

```bash
pip install black
```

To format your code, run the following command in the project's root directory:

```bash
black .
```
### Pre-Commit Hooks
We use pre-commit hooks to ensure that code formatting and other checks are performed automatically before each commit. To set up pre-commit hooks for this project, follow these steps:

Install the pre-commit package using pip:
```bash
pip install pre-commit
```

Run the following command in the project's root directory to install the pre-commit hooks:
```bash
pre-commit install
```

Now, the pre-commit hooks will run automatically before each commit, checking your code formatting and other requirements.

If you encounter any issues or have questions, feel free to reach out to the maintainers or open a new issue on GitHub. We're here to help and appreciate your efforts to contribute to the project.

Happy coding, and once again, thank you for your contributions!