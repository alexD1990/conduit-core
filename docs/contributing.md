# Contributing to Conduit Core

We welcome contributions from the community! Whether it's reporting a bug, proposing a new feature, improving documentation, or writing code, your help is valued.

## Getting Started

1.  **Fork the Repository:** Create your own copy of the Conduit Core repository on GitHub.
2.  **Clone Your Fork:** `git clone https://github.com/YOUR_USERNAME/conduit-core.git`
3.  **Set up Development Environment:**
    * Ensure you have Python 3.12+ and Poetry installed.
    * Navigate into the cloned directory: `cd conduit-core`
    * Install dependencies, including development tools: `poetry install`
    * Activate the virtual environment: `poetry shell`

## Contribution Workflow

1.  **Create a Branch:** Start from the `main` branch and create a descriptive branch for your changes:
    ```bash
    git checkout main
    git pull origin main # Ensure main is up-to-date
    git checkout -b feature/your-feature-name # Or fix/issue-number
    ```
2.  **Make Changes:** Write your code, tests, and documentation. Follow the existing code style and patterns.
3.  **Run Tests:** Ensure all tests pass before submitting:
    ```bash
    pytest
    ```
4.  **Commit Changes:** Use clear and concise commit messages. Reference issue numbers if applicable (e.g., `fix: Resolve issue #42 by handling edge case`).
    ```bash
    git add .
    git commit -m "feat: Add new connector for XYZ" -m "Detailed description..."
    ```
5.  **Push Branch:** Push your branch to your fork:
    ```bash
    git push origin feature/your-feature-name
    ```
6.  **Submit a Pull Request (PR):** Go to the main Conduit Core repository on GitHub. You should see a prompt to create a PR from your new branch. Fill out the PR template, explaining your changes and linking any relevant issues.

## Development Guidelines

* **Code Style:** Follow general Python best practices (PEP 8). We aim for clean, readable, and well-typed code.
* **Testing:** New features *must* include corresponding tests (unit or integration). Bug fixes should ideally include a test that reproduces the bug.
* **Documentation:** Update the `README.md` or files in the `/docs/` directory if your changes affect user-facing functionality or configuration.
* **Dependencies:** Be mindful when adding new dependencies. Discuss significant additions in an issue first.

## Areas for Contribution

* **New Connectors:** See the [Roadmap](roadmap.md) for desired connectors or propose your own. Follow the pattern in existing connectors (`src/conduit_core/connectors/`).
* **Bug Fixes:** Check the GitHub Issues for reported bugs.
* **Feature Enhancements:** Improve existing features based on the roadmap or your own ideas (discuss first!).
* **Documentation:** Improve clarity, add examples, or create tutorials.
* **Testing:** Increase test coverage, add tests for edge cases.

Thank you for helping make Conduit Core better!