## Setting up a Google Service Account and generating a json key

### Google Cloud project setup

<br>

1.	Log in to cloud.google.com with your google credentials
2.	On the top navigation bar (to the right), click on ‘Console’
3.	On the top navigation bar (to the left), click on the ‘Select a project’ dropdown and click ‘New Project’ on the top left of the pop-up window. 
4.	Enter your project name and leave Location as the default.
5.	Click ‘Create’; your project will be created and you will be redirected to your project dashboard.

<br>
  
### Creating a service key

1.	Click on the hamburger navigation menu at the top right of the dashboard
2.	Find ‘IAM & Admin’ and navigate to ‘Service Accounts’.
3.	Near the top of the Service Accounts pane, click ‘Create Service Account’.
4.	Give your service account a name (i.e. laura_serviceaccount). A Service Account ID will be generated. Optionally, give your service account a description.
5.	Click ‘Create and Continue’.
6.	Grant ‘BigQuery User’ AND 'BigQuery Data Viewer' permissions to this service account (filter to search list).
7.	Click continue
8.	Ignore step 3 and click ‘Done’.
9.	Your Service Account will now appear in the list in the Service Account pane. Click the three dots to the left under ‘Actions’ and select ‘Manage Keys’.
10.	In the new window, click ‘Add Key’ and select ‘Create new key’.
11.	Ensure the key type is ‘JSON’ and click ‘Create’.
12.	Your json key will be downloaded. Save in a secure location for use later on!
