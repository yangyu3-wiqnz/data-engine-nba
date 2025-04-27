This is a template for analytical repositories. To start using it, click button on eb-page "Use this template".
Replace all mentions of "data-engine-nba" in repository to the new repository name

Owner: _Code Owners (team who maintains this repository)_

Contact details: _Email of maintainers_

# Project Description

Project description: _Insert project description_

# Working with this project

## Option 1: Setting up cloud environment
In GCP we have development environment with already set up python and github integration. To set up dev environment:
1. Open Cloud Shell
2. Click button "Open Editor". This will open VSCode Editor. It may take some time if you never used Cloud Shell before
![alt text](./docs/image.png)
3. Clone repository from github with command (accept everything suggested by authorization process): 
```
git clone https://github.com/yangyu3-wiqnz/data-engine-nba
```
4. Change folder to `data-engine-nba`: 
```
cd data-engine-nba
```
5. Make sure that you have an access to Analytical sandbox. Run script `setup_local_env.sh`: 
```
. setup_local_env.sh
```

Script will install dbt and necessary dependencies, pre-commits. 
It will create a profile.yml file for dbt connection in proper folder and dataset in Analitycal sandbox with proper expiration date, time travel settings and description

**Work should start from changing folder to repo one and activating your virtual environment**:
```
cd data-engine-nba
source venv/bin/activate
```

**Note**: before pushing your first code to Github, you need to provide your username and email:
```
git config --global user.email "your_email@woolworths.co.nz"
git config --global user.name "Your Name"
```

## Option 2: Setting up local environment

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
