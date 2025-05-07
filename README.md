# Project Description

This is a repositories for setting up local solution for data-engine-nba.It uses Docker to set up airflow application which includes a few containers like airflow scheduler, triggerer etc. 

# Working with this project


##Setting up local environment

Please follow this link for instructions on how to set up local environment: [Local set up](./docs/setting_up_local_environment.md)

## Development workflow
Suggested workflow to ensure code quality, consistency, and streamlined collaboration across the team: 
[please follow this link](./docs/development_workflow.md)


## Github commands - easy start

1. Start working on new task with pulling latest changes from  main branch:
```
git checkout main
git pull
```
2. Create new branch for the work on the new task (note ticket number in branch name):
```
git checkout -b MERCH-xxx_name_of_branch
```
3. Once work is completed, commit your change and push it to remote repository (note ticket number in commit message):
```
git add .
git commit -m "MERCH-xxx: meaningful message"
# For first push
git push --set-upstream origin <name of your branch> 
# Or if not the first push simpler version
git push
```
4. If developer wants to bring new changes from main (or any other) branch into current one, they can do this with merge command:
```
git fetch origin
git merge origin/main
```
