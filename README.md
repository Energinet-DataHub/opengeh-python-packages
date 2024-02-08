# Welcome

This Readme is a template intended to understand initially how to structure assets that will live inside this repository.

Maintenance tasks from a platform perspective as well as onboarding of developers into the contents of a repository will be much easier if the folder/file structure for all repositories are aligned across product teams.

Product teams should avoid the temptation to define their own structure but align their ways of working to fit the structure outlined here.

## Folder structure

```txt
.
├── .github/
│   ├── workflows/
│   │   └── dependabot.yml
│   └── CODEOWNERS
│
├── docs/
│
├── source/
│   ├── spark_sql_migrations/
│   │   ├── spark_sql_migrations/
│   │   └── tests/
│   └── telemetry/
│       ├── telemetry/
│       └── tests/
│
├── .gitignore
├── LICENSE.md
└── README.md

(tree structure was built using https://tree.nathanfriend.io/)
```

## Files in root

### .github/workflows/ci.yml

Skeleton for Continuous Integration. CI should at least trigger on commits to branches with a PR intended to be merged to main

### .github/workflows/cd.yml

Skeleton for dispatch of event to dh3-environments after a PR has been merged

### .github/workflows/dependabot.yml

[Dependabot](https://github.com/dependabot) is configured for automated dependency updates for Nuget packages in source/dotnet folder

### .github/CODEOWNERS

The Outlaws platform team should be codeowners in .github folder to allow The Outlaws to do maintenance on product team's CI and CD pipelines without interrupting product teams unnecessarily

## Folders in root

### docs

Documentation folder should contain all necessary documentation to onboard developers into the contents and software architecture of this repository

### source

The source folder contains all the packages that are part of the repository.
Each package should have its own folder and inside that folder,
there should be a folder for the package and a folder for tests.
