# Contributing to Conduit Core

Thank you for your interest in contributing to Conduit Core! This guide will help you get started with development, understand our workflow, and make meaningful contributions.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Environment](#development-environment)
3. [Project Structure](#project-structure)
4. [Development Workflow](#development-workflow)
5. [Testing](#testing)
6. [Code Style & Standards](#code-style--standards)
7. [Contribution Types](#contribution-types)
8. [Pull Request Process](#pull-request-process)
9. [Release Process](#release-process)
10. [Community & Communication](#community--communication)

---

## Getting Started

### Prerequisites

**Required:**
- Python 3.12 or higher
- [Poetry](https://python-poetry.org/) (dependency management)
- Git
- GitHub account

**Recommended:**
- VS Code or PyCharm
- Docker (for integration tests)
- AWS CLI (for S3 connector development)

### Fork & Clone

1. **Fork the repository** on GitHub
2. **Clone your fork:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/conduit-core.git
   cd conduit-core
   ```

3. **Add upstream remote:**
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/conduit-core.git
   ```

---

## Development Environment

### Quick Setup

```bash
# Install dependencies (including dev tools)
poetry install

# Activate virtual environment
poetry shell

# Verify installation
conduit --version
pytest --version
```

### Environment Configuration

Create a `.env` file for local testing:

```bash
# Database credentials (optional, for connector testing)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_pass
POSTGRES_DB=test_db

# AWS credentials (optional, for S3 testing)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1

# Snowflake (optional)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Logging
LOG_LEVEL=DEBUG
```

### IDE Configuration

**VS Code (`settings.json`):**
```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.formatting.provider": "black",
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": false,
  "python.linting.flake8Enabled": true,
  "python.testing.pytestEnabled": true,
  "python.testing.unittestEnabled": false,
  "editor.formatOnSave": true,
  "editor.rulers": [88, 120]
}
```

**PyCharm:**
- Set interpreter: Poetry Environment
- Enable pytest as test runner
- Configure line length: 88 (Black standard)

---

## Project Structure

```
conduit-core/
├── src/conduit_core/          # Main package
│   ├── connectors/            # Data adapters
│   │   ├── base.py           # Abstract base classes
│   │   ├── registry.py       # Connector factory
│   │   ├── csv.py            # File connectors
│   │   ├── postgresql.py     # Database connectors
│   │   └── utils/
│   │       └── retry.py      # Shared utilities
│   ├── engine.py              # Pipeline orchestration
│   ├── cli.py                 # Typer CLI
│   ├── config.py              # Pydantic models
│   ├── schema*.py             # Schema subsystem
│   ├── quality.py             # Data quality framework
│   ├── manifest.py            # Audit trail
│   ├── checkpoint.py          # Resume logic
│   ├── state.py               # Incremental sync
│   └── errors.py              # DLQ & exceptions
│
├── tests/                     # Test suite (141+ tests)
│   ├── conftest.py           # Shared fixtures
│   ├── test_*.py             # Unit tests
│   ├── integration/          # E2E tests
│   └── connectors/           # Connector-specific tests
│
├── docs/                      # Documentation
│   ├── architecture.md
│   ├── data-quality.md
│   ├── contributing.md (this file)
│   └── *.md
│
├── pyproject.toml             # Poetry config
├── poetry.lock                # Locked dependencies
├── README.md                  # Project overview
├── LICENSE                    # BSL 1.1
└── .gitignore
```

---

## Development Workflow

### 1. Create a Branch

**Branch Naming Convention:**
```
feature/<short-description>     # New features
fix/<issue-number>-<description> # Bug fixes
docs/<description>              # Documentation
refactor/<description>          # Code refactoring
test/<description>              # Test improvements
```

**Examples:**
```bash
git checkout -b feature/mysql-connector
git checkout -b fix/42-null-handling
git checkout -b docs/update-readme
```

**Start from latest main:**
```bash
git checkout main
git pull upstream main
git checkout -b feature/your-feature
```

### 2. Make Changes

**Follow these principles:**
- **Single Responsibility:** One feature/fix per PR
- **Incremental:** Make small, reviewable commits
- **Test-Driven:** Write tests before or alongside code
- **Documentation:** Update docs for user-facing changes

### 3. Commit Changes

**Commit Message Format:**
```
<type>: <subject>

<body (optional)>

<footer (optional)>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Test additions/modifications
- `refactor`: Code restructuring (no behavior change)
- `perf`: Performance improvements
- `chore`: Tooling, dependencies, build changes

**Examples:**
```bash
# Good commits
git commit -m "feat: Add MySQL source connector"
git commit -m "fix: Handle null values in CSV parser"
git commit -m "docs: Update schema evolution examples"

# With body
git commit -m "feat: Add BigQuery destination

Implements BigQuery Load Jobs API for efficient bulk loading.
Supports full_refresh and append modes.
Auto-detects schema from source.

Closes #127"
```

### 4. Keep Branch Updated

```bash
# Fetch latest changes
git fetch upstream

# Rebase on main (preferred)
git rebase upstream/main

# Or merge (if rebase conflicts are complex)
git merge upstream/main

# Push to your fork
git push origin feature/your-feature --force-with-lease
```

---

## Testing

### Test Categories

1. **Unit Tests** (`tests/test_*.py`)
   - Test individual functions/classes in isolation
   - Fast, no external dependencies
   - Mock connectors, file I/O, network calls

2. **Integration Tests** (`tests/integration/`)
   - Test full pipeline flows
   - Use real connectors (file-based) or mocked external services
   - Verify end-to-end behavior

3. **Connector Tests** (`tests/connectors/`)
   - Test individual connector implementations
   - May require external services (Postgres, S3)
   - Use `pytest.mark.skipif` for missing credentials

### Running Tests

**All tests:**
```bash
pytest
```

**Specific test file:**
```bash
pytest tests/test_schema_validation.py
```

**Specific test function:**
```bash
pytest tests/test_quality_checks.py::test_regex_validator_email
```

**With coverage:**
```bash
pytest --cov=conduit_core --cov-report=html
open htmlcov/index.html
```

**Skip integration tests:**
```bash
pytest -m "not integration"
```

**Run only fast tests:**
```bash
pytest -m "not slow"
```

**Verbose output:**
```bash
pytest -v -s
```

### Writing Tests

**Example Unit Test:**
```python
# tests/test_quality_checks.py
import pytest
from conduit_core.quality import regex_validator

def test_regex_validator_email():
    """Test email regex validation"""
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    
    assert regex_validator("test@example.com", pattern=pattern) is True
    assert regex_validator("invalid-email", pattern=pattern) is False
    assert regex_validator(None, pattern=pattern) is False
```

**Example Integration Test:**
```python
# tests/integration/test_csv_to_json.py
import pytest
from pathlib import Path
from conduit_core.engine import run_resource
from conduit_core.config import IngestConfig

def test_csv_to_json_pipeline(tmp_path):
    """Test full CSV → JSON pipeline"""
    # Setup
    source_csv = tmp_path / "input.csv"
    source_csv.write_text("id,name\n1,Alice\n2,Bob\n")
    
    dest_json = tmp_path / "output.json"
    
    config = IngestConfig(
        sources=[{"name": "src", "type": "csv", "path": str(source_csv)}],
        destinations=[{"name": "dest", "type": "json", "path": str(dest_json)}],
        resources=[{"name": "test", "source": "src", "destination": "dest", "query": "n/a"}]
    )
    
    # Execute
    run_resource(config.resources[0], config)
    
    # Assert
    assert dest_json.exists()
    import json
    data = json.loads(dest_json.read_text())
    assert len(data) == 2
    assert data[0]["name"] == "Alice"
```

**Test Fixtures (`conftest.py`):**
```python
import pytest
from pathlib import Path

@pytest.fixture
def tmp_csv_file(tmp_path):
    """Create temporary CSV for testing"""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
    return csv_file

@pytest.fixture
def sample_config():
    """Sample IngestConfig for testing"""
    return IngestConfig(
        sources=[{"name": "test_src", "type": "csv", "path": "test.csv"}],
        destinations=[{"name": "test_dest", "type": "json", "path": "output.json"}],
        resources=[{"name": "test", "source": "test_src", "destination": "test_dest", "query": "n/a"}]
    )
```

### Test Requirements

**All PRs must:**
- ✅ Pass all existing tests
- ✅ Include tests for new features
- ✅ Include tests for bug fixes (reproduce bug → fix → verify)
- ✅ Maintain or improve code coverage
- ⚠️ Mark external-dependency tests with `@pytest.mark.skipif`

---

## Code Style & Standards

### Python Style

**We follow:**
- **PEP 8** (Python style guide)
- **PEP 484** (Type hints)
- **Black** (code formatter, 88 char line length)

**Format code:**
```bash
# Auto-format entire codebase
black src/ tests/

# Check without modifying
black --check src/ tests/
```

### Type Hints

**Always use type hints:**
```python
# Good
def validate_schema(source: Dict[str, Any], dest: Dict[str, Any]) -> ValidationReport:
    """Validate schema compatibility"""
    ...

# Bad
def validate_schema(source, dest):
    ...
```

**Type hint guidelines:**
- Use `typing` module (`Dict`, `List`, `Optional`, etc.)
- Use `Any` sparingly
- Use `Union` for multiple types
- Return types are mandatory
- Parameter types are mandatory

### Docstrings

**Format (Google style):**
```python
def process_batch(records: List[Dict[str, Any]], validator: QualityValidator) -> BatchResult:
    """Process a batch of records with quality validation.
    
    Args:
        records: List of record dictionaries to process
        validator: Configured QualityValidator instance
        
    Returns:
        BatchResult with valid/invalid record counts
        
    Raises:
        DataQualityError: If critical validation fails
        
    Example:
        >>> validator = QualityValidator(checks)
        >>> result = process_batch(records, validator)
        >>> print(f"Processed {result.valid_count} valid records")
    """
    ...
```

### Code Organization

**Module-level structure:**
```python
"""
Module docstring explaining purpose.
"""

# Standard library imports
import os
from typing import Dict, List, Any

# Third-party imports
from pydantic import BaseModel
import pandas as pd

# Local imports
from .base import BaseConnector
from .utils import retry_with_backoff

# Constants
DEFAULT_BATCH_SIZE = 1000
MAX_RETRIES = 3

# Type aliases
SchemaDict = Dict[str, Any]

# Classes and functions
class MyConnector(BaseConnector):
    ...
```

### Logging

**Use the logging utility:**
```python
from conduit_core.logging_utils import get_logger

logger = get_logger(__name__)

# Logging levels
logger.debug("Detailed debug information")
logger.info("General information")
logger.warning("Something unexpected but handled")
logger.error("Error occurred", exc_info=True)
```

### Error Handling

**Custom exceptions:**
```python
from conduit_core.errors import (
    DataQualityError,
    SchemaValidationError,
    ConnectorError
)

# Raise with context
if not valid:
    raise DataQualityError(
        f"Record {record_id} failed validation: {reason}"
    )

# Catch specific exceptions
try:
    result = connector.write(records)
except ConnectorError as e:
    logger.error(f"Write failed: {e}")
    # Handle gracefully
```

---

## Contribution Types

### 1. New Connectors

**Steps to add a connector:**

1. **Create connector file:**
   ```bash
   # For source connector
   touch src/conduit_core/connectors/mysql.py
   
   # For destination connector (or bidirectional)
   # Same file can contain both
   ```

2. **Inherit from base:**
   ```python
   from .base import BaseSource, BaseDestination
   from typing import Iterator, Dict, Any
   
   class MySQLSource(BaseSource):
       """MySQL source connector"""
       
       def read(self, query: str = None) -> Iterator[Dict[str, Any]]:
           """Read data from MySQL"""
           # Implementation
           yield record
       
       def test_connection(self) -> bool:
           """Test MySQL connection"""
           # Implementation
           return True
       
       def estimate_total_records(self) -> int:
           """Estimate record count for progress bar"""
           # Optional
           return None
   ```

3. **Add tests:**
   ```bash
   touch tests/connectors/test_mysql_connector.py
   ```

4. **Update documentation:**
   - Add to `docs/connectors.md`
   - Update README connector table
   - Add configuration example

5. **Add to roadmap** (if not already there)

**Connector Requirements:**
- ✅ Implement all abstract methods
- ✅ Handle errors gracefully
- ✅ Support retry for transient failures
- ✅ Include connection test
- ✅ Add comprehensive tests
- ✅ Document configuration
- ✅ Update README

### 2. Bug Fixes

**Bug fix workflow:**

1. **Create issue** (if not exists)
   - Describe bug behavior
   - Provide minimal reproduction
   - Include error messages

2. **Write test that reproduces bug:**
   ```python
   def test_bug_42_null_handling():
       """Test that null values don't crash validator
       
       Reproduces bug #42
       """
       records = [{"id": None, "name": "Alice"}]
       validator = QualityValidator([QualityCheck(column="id", check="not_null")])
       
       # Should not raise, should return validation result
       result = validator.validate_batch(records)
       assert len(result.invalid_records) == 1
   ```

3. **Fix the bug**

4. **Verify test passes**

5. **Add regression test** if needed

### 3. Features

**Feature development:**

1. **Discuss first**
   - Open GitHub issue
   - Describe use case
   - Propose implementation

2. **Get maintainer approval**

3. **Implement with tests**

4. **Update documentation**

### 4. Documentation

**Documentation improvements:**
- Fix typos
- Add examples
- Clarify confusing sections
- Create tutorials
- Add diagrams

**Docs live in:**
- `README.md` - Overview
- `docs/*.md` - Detailed guides
- Docstrings - Inline code docs

### 5. Testing

**Test contributions:**
- Increase coverage
- Add edge case tests
- Improve test utilities
- Add integration tests

---

## Pull Request Process

### Before Submitting

**Checklist:**
- [ ] Code follows style guidelines
- [ ] All tests pass (`pytest`)
- [ ] New tests added for new features/fixes
- [ ] Documentation updated
- [ ] Commit messages are clear
- [ ] Branch is up-to-date with main
- [ ] No merge conflicts

### Submitting PR

1. **Push to your fork:**
   ```bash
   git push origin feature/your-feature
   ```

2. **Create PR on GitHub**
   - Use descriptive title
   - Fill out PR template
   - Link related issues
   - Add screenshots/examples if applicable

3. **PR title format:**
   ```
   feat: Add MySQL source connector
   fix: Handle null values in CSV parser (#42)
   docs: Update schema evolution guide
   ```

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Refactoring
- [ ] Test improvement

## Related Issues
Closes #42

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] Tests added
- [ ] All CI checks pass
```

### Review Process

1. **Automated checks run:**
   - Pytest suite
   - Code style (Black)
   - Type checking (optional)

2. **Maintainer review:**
   - Code quality
   - Architecture fit
   - Test coverage
   - Documentation

3. **Address feedback:**
   ```bash
   # Make changes
   git add .
   git commit -m "Address review feedback"
   git push origin feature/your-feature
   ```

4. **Approval & merge:**
   - Maintainer approves
   - Squash & merge (usually)
   - Delete branch

---

## Release Process

*For maintainers*

### Version Numbering

**Semantic Versioning (SemVer):**
```
MAJOR.MINOR.PATCH

1.0.0 → 1.1.0 → 1.1.1 → 2.0.0
```

- **MAJOR:** Breaking changes
- **MINOR:** New features (backward compatible)
- **PATCH:** Bug fixes

### Release Steps

1. **Update version:**
   ```bash
   # pyproject.toml
   version = "1.1.0"
   ```

2. **Update CHANGELOG.md:**
   ```markdown
   ## [1.1.0] - 2025-11-15
   
   ### Added
   - MySQL source connector
   - Schema evolution auto mode
   
   ### Fixed
   - Null handling in CSV parser (#42)
   
   ### Changed
   - Improved error messages
   ```

3. **Create release commit:**
   ```bash
   git commit -m "chore: Release v1.1.0"
   git tag v1.1.0
   git push origin main --tags
   ```

4. **Build & publish:**
   ```bash
   poetry build
   poetry publish
   ```

5. **Create GitHub release:**
   - Use tag
   - Copy CHANGELOG entry
   - Attach artifacts (optional)

---

## Community & Communication

### Getting Help

**Channels:**
- GitHub Issues - Bug reports, feature requests
- GitHub Discussions - Questions, ideas
- Discord (if available) - Real-time chat

**When asking for help:**
- Provide context (OS, Python version, config)
- Include minimal reproduction
- Show error messages
- Describe expected vs actual behavior

### Code of Conduct

**We value:**
- Respect and kindness
- Constructive feedback
- Inclusivity
- Collaboration

**We don't tolerate:**
- Harassment or discrimination
- Aggressive or dismissive communication
- Spam or self-promotion

### Recognition

**Contributors are:**
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Eligible for maintainer role (after consistent contributions)

---

## Quick Reference

### Common Commands

```bash
# Setup
poetry install
poetry shell

# Testing
pytest                          # All tests
pytest -v                       # Verbose
pytest --cov                    # With coverage
pytest tests/test_file.py       # Specific file

# Code Quality
black src/ tests/               # Format code
black --check src/              # Check format

# Git
git checkout -b feature/name    # New branch
git add .                       # Stage changes
git commit -m "feat: message"   # Commit
git push origin feature/name    # Push
```

### Branch Naming

```
feature/<description>
fix/<issue>-<description>
docs/<description>
test/<description>
refactor/<description>
```

### Commit Types

```
feat, fix, docs, test, refactor, perf, chore
```

---

## Thank You!

Thank you for contributing to Conduit Core! Your efforts help make data ingestion more reliable and accessible for everyone.

**Questions?** Open a GitHub Discussion or Issue.

**Ready to contribute?** Pick an issue labeled `good first issue` or `help wanted`.

---

*This guide reflects Conduit Core v1.0. It may be updated in future versions.*
