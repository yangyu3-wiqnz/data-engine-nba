## Setting up local environment
### Install software
Install the following software if not already available:
- Git  ([Download Git](https://git-scm.com/download/))
- Python 3.10 or newer ([Download Python](https://www.python.org/downloads/))
- VSCode ([Download VSCode](https://code.visualstudio.com/download))
- Google Cloud SDK ([Install the gcloud CLI](https://cloud.google.com/sdk/docs/install))

### VSCode set up
Install the extensions:
https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user

### Google Cloud SDK set up
- Open terminal/CLI and run: `gcloud init`
- Complete the authorization step when prompted. 
- Choose a current Google Cloud project if prompted (Analytical Sandbox, gcp-wow-cd-email-app-test?).
- Choose a default Compute Engine zone if prompted.
- Run command below to create default credentials for apps:
```
gcloud auth application-default login
```
### Git set up
Determine where you want to save and work on local copies of Github repositories e.g. ~/Documents/Countdown/Git/

Navigate to the above location on a command line then "clone" the project with the command:  
```
git clone https://github.com/WoWNZ/wwnz-ai-analytical-project-template
```
This will create a new folder containing a copy of this Git repository.

### Python virtual environment set up
A python virtual environment is a sandboxed copy of python where you can install additional components specific to this
project.

- Navigate to the root folder of the cloned project (`cd wwnz-ai-analytical-project-template`)
- Create a python virtual environment: `python -m venv ./venv`
- Activate the virtual environment (this varies slightly depending on OS and terminal used):

For Windows CLI or Powershell: `.\venv\Scripts\activate`

Most command lines and IDEs will recognise a python virtual environment and give you a visual indication of which one
is currently active e.g. VSCode
- Install python requirements: `pip install -r requirements.txt`
- Install pre-commits: `pre-commit install`

### dbt profile setup
The dbt profile is a configuration file that stores the connection settings dbt needs to connect to one or more target 
databases (or "projects" in BigQuery).

The dbt project is in a subfolder of the repository: wwnz-ai-analytical-project-template/analytics_dbt_starter

Steps:

- Copy the profiles.yml file located in `tableau_semantic/profiles_example/`
- Paste it into the home directory of your user e.g.:
  - Windows: `C:\Users\<User>\.dbt\profiles.yml`
  - Mac: `~/.dbt/example_profiles.yml`
- The content of profiles.yml should look something like this:
```
analytics_dbt_starter:
  target: local
  outputs:
    local:
      type: bigquery
      method: oauth
      threads: 8
      project: gcp-wow-cd-email-app-test
      dataset: dbt_dev__<username>
      location: US
      priority: interactive
      retries: 1
```
- Replace *username* with some form of your name e.g. dbt_dev__thanos or dbt_dev__bruceb
- Manually create a BigQuery dataset with the above name in the Data Engine DEV project. This will be your personal 
dbt development dataset.
- Set dataset expiration date to 14 days, time travel period to 2 days.
- Create label "owner" and set email of the owner as the value, for example, 'first_name' or 'team_name'

### Testing the setup
- Ensure your python virtual environment is active
- Navigate to the dbt project subfolder
- Run the debug command: `dbt debug`
- Install dbt packages: `dbt deps` 
