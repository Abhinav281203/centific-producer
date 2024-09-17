import streamlit as st
from streamlit_autorefresh import st_autorefresh
import os
from datetime import datetime
from app.services import bricks_auth
from app.services import workspace
from app.services import jobs
from app.services import clusters
from config import development
import app.utils.lyra as lyra


def get_lyra_response(user_input):
    return lyra.messageGPT(db=st.session_state.db, message=user_input, thread_id=st.session_state.thread)['data']


def authenticate():
    """Function to handle Databricks authentication."""
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        return True

    db_object = bricks_auth.bricks_object(host=development.DATABRICKS_HOST, 
                                            token=development.DATABRICKS_TOKEN)
    if bricks_auth.verify(db_object):
        st.session_state.authenticated = True
        st.session_state.db = db_object
        st.success("Login successful!")
            
        if 'workspace_files' not in st.session_state:
            st.session_state.workspace_files = []
            st.session_state.workspace_files = fetch_workspace()
        
        if 'cluster_ids' not in st.session_state:
            st.session_state.cluster_ids = []
            st.session_state.cluster_ids = fetch_clusters()

        if 'existing_jobs' not in st.session_state:
            st.session_state.existing_jobs = []
            st.session_state.existing_jobs = fetch_existing_jobs()

        if 'thread' not in st.session_state:
            thread = lyra.getaThread()
            st.session_state.thread = thread
        return True
    else:
        st.error("Login Failed")
        return False
    
def fetch_workspace():
    workspace_files = workspace.all_files(db=st.session_state.db, path="/")
    if workspace_files["status"]:
        return workspace_files["data"]
    else:
        return []
    
def fetch_clusters():
    clusters_list = clusters.list_clusters(db=st.session_state.db, needs=set(["cluster_id", "cluster_name"]))
    if clusters_list["status"]:
        return clusters_list["data"]
    else:
        return []

def fetch_existing_jobs():
    existing_jobs = jobs.all_jobs(db=st.session_state.db, needs=set(["job_id", "settings"]))
    if existing_jobs["status"]:
        return existing_jobs["data"]
    else:
        return []

def workspace_screen():
    """Displays the Workspace screen and handles file upload to Databricks."""
    st.title("Workspace")

    # Fetch and display workspace files
    st.session_state.workspace_files = fetch_workspace()
    files = st.session_state.workspace_files
    if len(files) > 0:
        st.markdown("### Workspace Files:")
        st.markdown("<ul>", unsafe_allow_html=True)
        for path in files:
            st.markdown(f"<li>{path}</li>", unsafe_allow_html=True)
        st.markdown("</ul>", unsafe_allow_html=True)

    else:
        st.error("Failed to retrieve workspace files.")

    st.header("Upload File")

    # File upload functionality
    uploaded_file = st.file_uploader("Upload a file")
    
    if uploaded_file is not None:
        try:
            # Save the uploaded file to a local directory
            local_path = os.path.join("uploads", uploaded_file.name)
            with open(local_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            # After file is uploaded, ask for the workspace path
            upload_path = st.text_input("Enter the path to upload the file in Databricks workspace")

            if upload_path:
                upload_button = st.button("Upload")

                if upload_button:
                # Upload the file to the Databricks workspace
                    contents = workspace.read_ipynb(local_path=local_path)
                    uploaded = workspace.create_notebook(db=st.session_state.db, path=upload_path, content=contents)

                    if uploaded["status"]:
                        st.success(f"File successfully uploaded to Databricks workspace at: {upload_path}")
                        st.session_state.workspace_files = fetch_workspace()
                        st.rerun()
                    else:
                        st.error("Failed to upload file to Databricks workspace.")
                    
                    # Cleanup: Remove the local file after it has been uploaded to the workspace
                    os.remove(local_path)
        except Exception as e:
            st.error(f"An error occurred during file processing: {e}")
    else:
        st.info("Please upload a file to proceed.")

def jobs_screen():
    """Displays the Jobs screen with dynamic 'Add' functionality."""
    st.title("Jobs")
    st.session_state.existing_jobs = fetch_existing_jobs()
    if "selected_task" not in st.session_state:
        st.session_state.selected_task = None

    show_existing_tasks = st.checkbox("Show Existing Jobs")

    if show_existing_tasks:
        ids = []
        jobs_list = []
        for i in st.session_state.existing_jobs:
            ids.append(i["job_id"])
            jobs_list.append(i["settings"]["name"])

        select_job = st.selectbox("Select existing Jobs", jobs_list)
        if select_job:
            index = jobs_list.index(select_job)
            st.session_state.selected_task = ids[index]
    else:

    # Initialize session state for task details if not already present
        if "task_names" not in st.session_state:
            st.session_state.task_names = []
        if "notebook_paths" not in st.session_state:
            st.session_state.notebook_paths = []
        if "cluster_ids" not in st.session_state:
            st.session_state.cluster_ids = fetch_clusters()  # Fetch cluster IDs dynamically
        if "cluster_ids_form" not in st.session_state:
            st.session_state.cluster_ids_form = []  # Use this for form-specific cluster IDs
        if "dependencies" not in st.session_state:
            st.session_state.dependencies = []


        # Job name input
        job_name = st.text_input("Enter Job Name")

        st.header("Add a Task")

        # Form to add tasks
        with st.form(key="job_form"):
            task_name = st.text_input("Task Name", placeholder="Enter the task name")
            notebook_path = st.selectbox("Notebook Path", options=st.session_state.workspace_files)
            # notebook_path = st.text_input("Notebook Path", placeholder="Enter the notebook path for the task")
            cluster_id = st.selectbox("Enter the cluster ID for the task", options=st.session_state.cluster_ids)
            
            # List previous tasks for dependencies
            previous_tasks = st.session_state.task_names
            drop_down_deps = st.multiselect("Dependencies (Previous Tasks)", options=previous_tasks, default=[])

            # Add Task button inside the form
            if st.form_submit_button("Add Task"):
                if task_name and notebook_path and cluster_id:
                    # Append new task details to the session state lists
                    st.session_state.task_names.append(task_name)
                    st.session_state.notebook_paths.append(notebook_path)
                    st.session_state.cluster_ids_form.append(cluster_id["cluster_id"])
                    st.session_state.dependencies.append(drop_down_deps)
                    
                    # Clear the input fields and update the dependency dropdown
                    st.rerun()
                    
                    st.success(f"Task '{task_name}' added!")
                else:
                    st.error("Please fill all fields before adding the task.")

        # Display added tasks immediately in the order they were added
        if st.session_state.task_names:
            st.header("Tasks Added")
            for i, task in enumerate(st.session_state.task_names):
                st.write(f"**Task {i + 1}:**")
                st.write(f"- **Task Name:** {task}")
                st.write(f"- **Notebook Path:** {st.session_state.notebook_paths[i]}")
                st.write(f"- **Cluster ID:** {st.session_state.cluster_ids_form[i]}")
                st.write(f"- **Dependencies:** {', '.join(st.session_state.dependencies[i]) if st.session_state.dependencies[i] else 'None'}")
                st.write("---")  # Separator between tasks
        
        if st.session_state.task_names:
            create_button = st.button("Create")
            if create_button:
                created = jobs.create_job(
                    db=st.session_state.db, 
                    job_name=job_name,
                    task_names=st.session_state.task_names,
                    paths=st.session_state.notebook_paths,
                    dependents=st.session_state.dependencies,
                    cluster_ids=st.session_state.cluster_ids_form
                )

                st.session_state.task_names = []
                st.session_state.notebook_paths = []
                st.session_state.cluster_ids_form = []
                st.session_state.dependencies = []

                st.session_state.existing_jobs = fetch_existing_jobs()
                st.rerun()

                if created["status"]:
                    st.success(f"Job '{job_name}' has been created successfully!")
                    # print(created["data"])
                    st.session_state.selected_task = created["data"]["job_id"]
                else:
                    st.error("An error occurred while creating the job. Please try again.")
                    st.error(f"Error details: {created.get('data', 'No additional details available')}")

    if st.session_state.selected_task is not None:
        run_button = st.button("Run")    
        if run_button:
            runned = jobs.run_job(db=st.session_state.db, job_id=st.session_state.selected_task)
            if runned["status"]:
                st.success(f"Job with run id: {st.session_state.selected_task} has started successfully with run id: {runned['data']['run_id']}")
                st.session_state.selected_task = None
            else:
                st.error("Error occured, Try again.")

def convert_timestamp_to_datetime(timestamp):
    return datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

def convert_status(status):
    if status == "TERMINATED":
        return '<span style="color:red;">TERMINATED</span>'
    elif status == "RUNNING":
        return '<span style="color:green;">RUNNING</span>'
    elif status == "PENDING":
        return '<span style="color:orange;">PENDING</span>'
    elif status == "STARTING":
        return '<span style="color:blue;">STARTING</span>'
    elif status == "TERMINATING":
        return '<span style="color:orange;">TERMINATING</span>'
    elif status == "SUCCESS":
        return '<span style="color:green;">SUCCESS</span>'
    elif status == "FAILED":
        return '<span style="color:red;">FAILED</span>'
    elif status == "CANCELED":
        return '<span style="color:gray;">CANCELED</span>'
    elif status == "INTERNAL_ERROR":
        return '<span style="color:red;">INTERNAL ERROR</span>'
    elif status == "SKIPPED":
        return '<span style="color:gray;">SKIPPED</span>'
    else:
        return f'<span style="color:black;">{status}</span>'


def runs_and_status():
    """Displays the Runs and Status screen."""
    st.title("Runs and status")
    st.session_state.existing_jobs = fetch_existing_jobs()
    count = st_autorefresh(interval=5000, limit=None, key="auto-refresh")


    ids = []
    jobs_list = []
    for i in st.session_state.existing_jobs:
        ids.append(i["job_id"])
        jobs_list.append(i["settings"]["name"])

    job_id_for_run = st.selectbox("Select a Job", jobs_list)
    if job_id_for_run:
        index = jobs_list.index(job_id_for_run)
        job_id_for_run = ids[index]

    if job_id_for_run:
        runs = jobs.get_job_runs(db=st.session_state.db, job_id=job_id_for_run)
        if runs["status"]:
            runs_data = runs["data"]
            show_selected_run_dropdown = []
            for i in runs_data:
                datetime_readable = convert_timestamp_to_datetime(i['start_time'])
                show_selected_run_dropdown.append(f"Run initiated on {datetime_readable} UTC, Run ID : {i['run_id']}")
            
            selected_run = st.selectbox("Select a Run", show_selected_run_dropdown)
            selected_run_idx = show_selected_run_dropdown.index(selected_run)
            run_id = runs_data[selected_run_idx]["run_id"]

            if run_id:
                run_status = jobs.get_run(db=st.session_state.db, run_id=run_id)
                if run_status["status"]:
                    data = run_status["data"][0]
                    st.write(f"**Job ID** : {data['job_id']}")
                    st.write(f"**Run ID** : {data['run_id']}")
                    st.write("**Started on** :", convert_timestamp_to_datetime(data["start_time"]), "UTC")
                    st.write(f"**Execution Duration** : {data['execution_duration']}")
                    st.markdown(f"<p><strong>Job Status:</strong> {convert_status(data['status']['state'])}</p>", unsafe_allow_html=True)
                    if data["status"]["state"] == "TERMINATED":
                        st.info(data["status"]["termination_details"]["message"])

                    if data["format"] == "SINGLE_TASK":
                        st.subheader("Task :")
                        st.write("**Running** : ", data["task"]["notebook_task"]["notebook_path"])
                        st.write(f"**Cluster ID** :", data["cluster_instance"]["cluster_id"])
                        status = convert_status(data["status"]["state"])
                        st.write(f"**Status** : {status}")
                    else:
                        
                        for idx in range(len(data["tasks"])):
                            i = data["tasks"][idx]
                            st.subheader(f"Task {idx + 1} :")
                            # st.write(i)
                            st.write(f"**Task Key** : {i['task_key']}")
                            st.write(f"**Running** : {i['notebook_task']['notebook_path']}")
                            st.write(f"**Run ID** : {i['run_id']}")
                            st.write(f"**Cluster ID** :", i["existing_cluster_id"])
                            # print(idx+1, i['status']['state'])
                            st.markdown(f"<p><strong>Job Status:</strong> {convert_status(i['status']['state'])}</p>", unsafe_allow_html=True)
                            if i["status"]["state"] == "TERMINATED":
                                st.info(i["status"]["termination_details"]["message"])

                    # st.write(data)
                else:
                    st.error("Error while fetching runs. Please try again.")

        else:
            st.error("Error while fetching runs. Please try again.")


def ai():
    """Displays the AI screen."""
    # Custom CSS
    st.markdown("""
    <style>
    .stApp {
        background-color: #f0f2f6;
    }
    .stTextInput > div > div > input {
        background-color: #ffffff;
    }
    .stMarkdown {
        font-family: 'Arial', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

    # App header
    st.title("Talk with Lyra")
    st.markdown("***Databricks Bot***")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar="🧑‍💻" if message["role"] == "user" else "🤖"):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("What would you like to ask Lyra?"):
        # User message
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Lyra's response
        with st.spinner("Lyra is thinking..."):
            lyra_response = get_lyra_response(prompt)
        with st.chat_message("assistant", avatar="🤖"):
            st.write(lyra_response)
        st.session_state.messages.append({"role": "assistant", "content": lyra_response})

    # Add a footer
    st.markdown("---")
    st.markdown("Powered by Lyra AI | Created by Pipeline Pioneers")


def main():
    """Main function handling the navigation and authentication."""
    if authenticate():
        st.sidebar.title("Navigation")
        option = st.sidebar.radio("Choose a screen", ["Workspace", "Jobs", "Runs and status", "Lyra"])

        if option == "Workspace":
            workspace_screen()
        elif option == "Jobs":
            jobs_screen()
        elif option == "Runs and status":
            runs_and_status()
        elif option == "Lyra":
            ai()

if __name__ == "__main__":
    # Initialize session state for authentication status
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    # Create uploads directory if not exists
    if not os.path.exists("uploads"):
        os.makedirs("uploads")

    main()
