"""
template.py

Purpose:
    Automates the creation of project folders and files for the "us_used_cars_ml_pipeline" project.
    This script will scaffold a predefined project structure with folders and files.
    Existing files won't be overwritten.

Usage:
    Run this script in the desired location to scaffold the project structure.
    `python template.py`

Dependencies:
    - os, pathlib, logging
"""

import os
from pathlib import Path
import logging

# Logging setup to track the creation of directories and files.
logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s:')

project_name = "us_used_cars_ml_pipeline"

# GitHub Workflow: Contains configurations for GitHub actions that manage CI/CD.
list_of_files = [
    ".github/workflows/.gitkeep",
]

# App Code: Contains the main application logic and source code, organized modularly.
list_of_files.extend([
    f"src/{project_name}/__init__.py",
    f"src/{project_name}/components/__init__.py",
    f"src/{project_name}/pipelines/__init__.py",
    f"src/{project_name}/pipelines/data_splitter.py",
    f"src/{project_name}/pipelines/data_loader.py",
    f"src/{project_name}/models/__init__.py",
    f"src/{project_name}/models/ensemble_training.py",
    f"src/{project_name}/models/stacking_training.py",
    f"src/{project_name}/streaming/__init__.py",
    f"src/{project_name}/streaming/listener.py",
    f"src/{project_name}/streaming/metrics_calculator.py",
    f"src/{project_name}/config/__init__.py",
    f"src/{project_name}/config/configuration.py",
    f"src/{project_name}/entity/__init__.py",
    f"src/{project_name}/entity/config_entity.py",
    f"src/{project_name}/constants/__init__.py",
    
    # Configuration files at the root level
    "config/config.yaml",
    "params.yaml",
    "schema.yaml",
    
    # Primary application execution files
    "main.py",
    "app.py",
    "setup.py",
    ".gitignore"
])

# Kubernetes & Infrastructure: Configuration files and scripts related to Kubernetes deployments and management.
list_of_files.extend([
    f"k8s/deployments/.gitkeep",
    f"k8s/services/.gitkeep",
    f"k8s/configmaps/.gitkeep",
    f"k8s/secrets/.gitkeep",
    f"k8s/cronjobs/.gitkeep",
])

# CI/CD: Files related to Continuous Integration and Continuous Deployment.
list_of_files.extend([
    ".github/workflows/ci_cd.yaml",
    "scripts/build.sh",
    "scripts/deploy.sh",
])

# Docker: Configuration files for creating containerized versions of the application.
list_of_files.extend([
    "Dockerfile",
    ".dockerignore",
])

# Data: Contains the Kaggle dataset and any other necessary datasets.
list_of_files.extend([
    "data/.gitkeep",
])

# Kafka: Configurations related to Kafka for message streaming.
list_of_files.extend([
    "kafka/config/.gitkeep",
])

# Monitoring & Logging: Configuration files for setting up monitoring and logging services.
list_of_files.extend([
    "monitoring/prometheus.yaml",
    "logging/elasticsearch.yaml",
])

# Documentation: Contains detailed documentation of different components of the application.
# list_of_files.extend([
#     "docs/README.md",
# ])

# Miscellaneous: Contains utility scripts and unit tests for the application.
list_of_files.extend([
    "utils/__init__.py",
    "utils/common.py",
    "tests/__init__.py"
])

# Research: Contains notebooks and other scripts for exploratory work.
list_of_files.extend([
    "research/trials.ipynb",
    "research/data_exploration.ipynb",
    "research/model_prototyping.ipynb"
])

# Iterating through the list to create directories and files
for file_path_str in list_of_files:
    file_path = Path(file_path_str)
    
    # Create directory if it doesn't exist
    if file_path.parent and not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Creating directory: {file_path.parent}")

    # Create the file if it's non-existent or empty
    if not file_path.exists() or file_path.stat().st_size == 0:
        file_path.touch()
        logging.info(f"Creating empty file: {file_path}")
    else:
        logging.info(f"{file_path.name} already exists")
