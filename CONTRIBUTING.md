# Contributing to this project

Contributions to Chr.Avro are welcome! Before opening an issue or creating a pull request, please take a moment to review this document in order to make the contribution process easy and effective for everyone involved.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue or assessing patches and features.

These guidelines are adapted from Nicolas Gallagher’s [issue-guidelines](https://github.com/necolas/issue-guidelines).

## Using the issue tracker

The issue tracker on this repository is the preferred channel for [bug reports](#bug-reports), [feature requests](#feature-requests), and discussion prior to [pull requests](#pull-requests), but please respect the following restrictions:

*   Please **do not** use the issue tracker for personal support requests.

*   Please **do not** derail or troll issues. Keep the discussion on topic and respect the opinions of others.

## Bug reports

A bug is a _demonstrable problem_ that is caused by the code in the repository. Good bug reports are extremely helpful—thanks in advance!

Guidelines for bug reports:

1.  **Use the issue search** to check whether the same bug has already been reported.

2.  **Reproduce the bug against the main branch** to ensure that it hasn’t been fixed already.

3.  **Isolate the bug**—determine the minimal amount of code needed to reproduce the problem.

A good bug report should include everything needed for other contributors to reproduce the problem, so please try to be as detailed as possible. (What version of Chr.Avro are you using? What operating systems have you tested with? How would you expect Chr.Avro to behave?)

Example:

> **Short and descriptive title**
>
> Provide a summary of the issue and the environment in which it occurs. If necessary, include any steps required to reproduce the bug:
>
> 1.
> 2.
>
> Either link to a reduced test case or include the code here.
>
> Add any other information that may be relevant to the issue being reported. This might include lines of code that you’ve identified as problematic, workarounds that you’ve tried, and potential solutions (and your opinions on their merits).

## Feature requests

We encourage feature requests, but it’s up to you to make a strong case—please
provide as much detail and context as possible.

## Pull requests

Good pull requests—for patches, improvements, or new features—are a fantastic help. They should remain focused in scope and avoid containing unrelated commits.

Before embarking on any significant pull request (e.g., implementing features or refactoring code), **ask about it**: If you don’t, you risk spending a lot of time working on something that the project’s maintainers might not want to merge.

Please match the style used throughout the project (consistent indentation, accurate comments, etc.). We include an [.editorconfig](.editorconfig) file for the mundane stuff; use your best judgment for everything else.

To open a well-formed pull request:

1.  [Fork](http://help.github.com/fork-a-repo/) the project, clone your fork, and configure the remotes:

    ```bash
    # clone your fork of the repo into the current directory
    git clone https://github.com/<your-username>/dotnet-avro

    # navigate to the newly cloned directory
    cd dotnet-avro

    # assign the original repo to a remote called "upstream"
    git remote add upstream https://github.com/ch-robinson/dotnet-avro
    ```

2.  If it’s been awhile since you cloned the repo, make sure you’re working with the latest code:

    ```bash
    git checkout main
    git pull upstream main
    ```

3.  Create a new topic branch to contain your feature, change, or fix:

    ```bash
    git checkout -b <topic-branch-name>
    ```

4.  Commit your changes in logical chunks and make your commit messages consistent with the rest of the project. Use Git’s [interactive rebase](https://help.github.com/en/articles/about-git-rebase) feature to tidy up your commits before making them public.

5.  Before opening the PR, rebase your topic branch onto the upstream main branch:

    ```bash
    git pull --rebase upstream main
    ```

6.  Push your topic branch up to your fork:

    ```bash
    git push origin <topic-branch-name>
    ```

7.  [Open a pull pequest](https://help.github.com/articles/using-pull-requests/) against the main branch.

**Note**: By opening a pull request, you agree to allow C.H. Robinson to license your work under the same [license](LICENSE.md) used by the project.
