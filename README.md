Companion Repository - Practical Guide to Apache Airflow® 3 
============================================================

Welcome! This repository contains all code listings and the example pipeline for the Practical Guide to Apache Airflow® 3 book. 


It is also a functioning Ariflow project that you can try out locally with the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)! This project creates a personalized inspirational newsletter for a user based on their name, location and favorite sci-fi character.

![The example pipeline as shown in the book in chapter 4](/src/pipeline_diagram_chapter_4.png) 

Project Contents
================

This repository contains code listings, the example pipeline for each chapter, and an Airflow project containing the pipeline as shown at the end of chapter 4.

- .astro: Advanced configs. We recommend not modifying this folder.

- [code_examples](/code_examples/): This folder contains all code examples outside of the finished example pipeline. To run a dag from this folder, copy the code into the `dags` folder.
    - [all_listings](/code_examples/all_listings/): This folder contains all code listings from the book. Note that not all listings are complete dags.
    - [example_pipeline_chapter_versions](/code_examples/example_pipeline_chapter_versions/): This folder contains the state of the pipeline at the end of chapter 2, 3 and 4, as well as a version of the pipeline using Amazon SQS in an inference execution pattern as described in chapter 7. Note that to run the version from chapter 7, you need to have Amazon SQS and a valid AWS connection set up in your Airflow instance. See [the chapter 7 pipeline diagram](/src/pipeline_diagram_chapter_7.png) for more details.
    - [tradational_operator_examples](/code_examples/tradational_operator_examples/): This folder contains additional code examples not shown in the book that use traditional Airflow operators.

- [dags](/dags/): This folder contains two Python files containing the 4 dags that form the example pipeline as seen at the end of chapter 4. Note that you need to define an OpenAI connection to run the last dag.   
    - [create_newsletter.py](/dags/create_newsletter.py): Contains 3 dags defined in an asset-oriented approach using the `@asset` decorator (chapter 2). These three dags can be run without any other additional setup.
    - [personalize_newsletter](/dags/personalize_newsletter.py): Contains the personalization dag defined in a task-oriented approach using the `@dag` and `@task` decorators (chapter 2). The dag shown represents the state at the end of chapter 4. To view the state of the dag at the end of other chapters see [example_pipeline_chapter_versions](/code_examples/example_pipeline_chapter_versions/).

- [include](/include/): Contains supporting files for the project and is automatically included in the Docker image.  
    - [newsletter](/include/newsletter/): Contains the `newsletter_template.txt` used in the `create_newsletter.py` dag, as well as a sample generic and personalized newsletter created with the pipeline.
    - [user_data](/include/user_data/): Contains a sample json file for one newsletter subscriber. 

- [plugins](/plugins/): This folder contains custom plugins for your project. It is empty by default.
- [src](/src/): Contains images for the readme.
- [tests](/tests/): This folder contains tests for your project. A few sample dag validation tests are provided. You can run the tests with `astro dev pytest`.
- [.dockerignore](/.dockerignore): This file contains a list of files and folders that should be ignored when building the Docker image for your project. It is empty by default.
- [.env_example](/.env_example): This file contains a sample environment variable file for your project. To set your environment variables copy this file to `.env` and fill in the values that you need for your pipeline version.
- [.gitignore](/.gitignore): This file contains a list of files and folders that should be ignored by Git. 
- [Dockerfile](/Dockerfile): This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- [packages.txt](/packages.txt): Install OS-level packages needed for your project by adding them to this file. Contains `git` which is only needed if you want to configure a GitDagBundle.
- [README.md](/README.md): This file.
- [requirements.txt]: Install Python packages needed for your project by adding them to this file. Installs the providers and packages used in the example pipeline.
- [ruff.toml](/ruff.toml): This file contains the configuration to lint an Airflow 2 project to check for compatibility with Airflow 3.0.

Deploy Your Project Locally
===========================

1. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed and are at least on `astro version` 1.34.0. Upgrade with `brew upgrade astro`.
2. Fork and clone this repository to your local machine.
3. In the root of the project, run `astro dev start` to start the project locally.

This command will spin up 5 containers on your machine, each for a different Airflow component (see chapter 5):

- Postgres: Airflow's Metadata Database
- API Server: The Airflow component responsible for rendering the Airflow UI and serving 3 APIs, one of which is needed for task code to interact with the Airflow metadata database.
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Dag processor: The Airflow component responsible for parsing dags.
- Triggerer: The Airflow component responsible for triggering deferred tasks

Note: Running `astro dev start` will start your project with the Airflow UI exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

4. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/. 
5. Add a connection to OpenAI by going to Admin > Connections > + Add Connection, fill in the following fields (all other fields can be left empty) and click Save:

    - Connection ID: `my_openai_conn`
    - Connection Type: `openai`
    - Password: `<your OpenAI API key>` ([OpenAI API key](https://platform.openai.com/docs/api-reference/authentication))

6. Unpause all 4 dags in the Airflow UI by hitting the toggle to the right side of the screen. 
7. Trigger the `raw_zen_quotes` dag to create a manual run. All other dags will be triggered based on a data-aware schedule (chapter 3) as soon as their data is ready. 
8. Checkout the created generic and personalized newsletter in [include/newsletter](include/newsletter/). 

![Dags overview showing the 4 dags](/src/dags_overview.png)

Deploy Your Project to Astro
=============================

To deploy the project to Astro follow the instructions in chapter 6. A [free trial of Astro](https://www.astronomer.io/trial-3) is available.
