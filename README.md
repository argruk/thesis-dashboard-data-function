# thesis-dashboard-data-function

To have an application working, you also need to install two other projects:
1. [Server](https://github.com/argruk/thesis-toolset-backend)
2. [Frontend](https://github.com/argruk/dashboard-frontend)

This project also requires a `settings.json` file present in the root folder of this project.  
![credentialsExample](https://github.com/argruk/thesis-toolset-backend/assets/36072338/52603aab-7b37-4a51-bf63-99ceed3795dd)

A project for extracting data from Cumulocity.
Requires RabbitMQ running on port 5672.


## Installing RabbitMQ
1. Follow [this guide](https://www.rabbitmq.com/download.html) to install RabbitMQ.

## Installing the function

1. Clone this repository `git clone https://github.com/argruk/thesis-dashboard-data-function.git`.
2. Make sure you have Python 3.7.7 or higher. (RECOMMENDED: install [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) for Python)
3. Install necessary packages: `pip install -r requirements.txt`.
4. Add the settings.json file with your credentials to the root folder.
5. Make sure the RabbitMQ broker is running.
6. Start the application with `python main.py`.
