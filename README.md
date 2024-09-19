# Installation guide
### This section covers the basics of how to install this application

### Requirements for Installing Packages
*  **Python** : Make sure you have Python >= 3.10. You can install it using:
```
sudo apt update
sudo apt install python3.10
```
* **Install Required Packages** : Install the necessary packages listed in `requirements.txt`:
```
pip install -r requirements.txt
```
* **Configuration** : Provide the necessary credentials in ./config/development.py.




### Run the application
* To run the application for the web version (GUI), use the following command:
```
python -m streamlit run ./streamlit_app.py
```

* Alternatively, to host the endpoints, you can run:
```
python -m uvicorn run:app --reload
```
