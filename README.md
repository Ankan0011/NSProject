## Steps to build this project.

In order to replicate this project, please follow the below mentioned steps:
    
- Install all the necessary python packages. Install pip installer for python if necessary.
- Please create a seperate repository for each users to avoid single code base.

Please login into the VM using the ssh connection. Clone the project in a folder.

```
cd NSProject
sudo pip install -r requirements.txt
sudo python3 setup.py install
```

This commands will install all the python packages and install the helper functions in `/util` directory to be used in the code. I have created a sample code in sample folder. You can execute the code by following the below comment.

```
cd sample
python3 SCC.py
```

### Setup the project on VSCODE guildlines ###

I would prefer using vscode for the dev IDE and use `Remote Explorer` extension in vscode for the remote collab with codebase. You can also follow the below mentioned link for setting up with your personal laptop.

https://code.visualstudio.com/docs/remote/ssh-tutorial#_connect-using-ssh 
