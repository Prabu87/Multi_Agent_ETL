"""Setup script for development environment."""

import subprocess
import sys


def main():
    """Set up the development environment."""
    print("Setting up Multi-Agent ETL Platform development environment...")
    
    # Install dependencies
    print("\n1. Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."])
        print("✓ Core dependencies installed")
    except subprocess.CalledProcessError:
        print("✗ Failed to install core dependencies")
        return 1
    
    # Install dev dependencies
    print("\n2. Installing development dependencies...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "pytest", "hypothesis", "pytest-asyncio", "black", "mypy", "ruff"
        ])
        print("✓ Development dependencies installed")
    except subprocess.CalledProcessError:
        print("✗ Failed to install development dependencies")
        return 1
    
    # Run tests
    print("\n3. Running tests...")
    try:
        subprocess.check_call([sys.executable, "-m", "pytest", "tests/", "-v"])
        print("✓ All tests passed")
    except subprocess.CalledProcessError:
        print("✗ Some tests failed")
        return 1
    
    print("\n✓ Setup complete!")
    print("\nNext steps:")
    print("  - Review the README.md for usage instructions")
    print("  - Configure Terraform for infrastructure provisioning")
    print("  - Start implementing the ETL platform components")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
