"""
VizTracks App — a visualization generator powered by your Spotify history
Copyright (C) 2025 Maxwell Bernard


Disclaimer: Spotify is a registered trademark of Spotify AB.
This app is a third-party tool that uses Spotify data and is not affiliated with or endorsed by Spotify

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""

import base64
import os
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests
import streamlit as st

from src.data.normalize_inputs import normalize_inputs
from src.services.supabase_client import supabase
from src.visuals.anims.create_bar_animation import (
    days,
    dpi,
    figsize,
    interp_steps,
    period,
)

# Packaging settings for JSON uploads
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

st.set_page_config(
    page_title="VizTracks",
    page_icon="🎵",
    layout="wide",
)

# custom CSS to style the app
st.markdown(
    """
    <style>
    
    /* Page layout adjustments */
    .stApp {
        margin-top: -52px !important;
        # background: #ecedeb !important;
        background: #e2e3e1 !important;
        # background: #fafafa !important;

    }
    
    /* Sidebar spacing */
    section[data-testid="stSidebar"] {
        margin-top: -55px !important;
        width: 325px !important;
    }
    
    /* Style the entire sidebar */
    section[data-testid="stSidebar"] > div:first-child {
        background: linear-gradient(180deg, #1DB954, #1ED760, #6FFEA4) !important;
        padding: 15px !important;
        margin: 0px !important;
        box-shadow: 0 0 12px rgba(0,0,0,0.3) !important;
    }
    
    # /* Video borders */
    # video[controls] {
    #     border: 2px solid #d3d3d3 !important;
    #     border-radius: 10px !important;
    #     box-shadow: 0 2px 6px rgba(0, 0, 0, 0.10) !important;
    # }
    
    /* Header background */
    header {
        # background-color: #fafafa !important;
        background-color: #e2e3e1 !important;

    }
    
    /* Content width limit */
    .block-container {
        max-width: 1300px !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }
    
    /* Form styling */
    div[data-testid="stForm"] {
        border: none !important;
        box-shadow: none !important;
    }
    
    /* Sidebar form button styling */
    section[data-testid="stSidebar"] .stFormSubmitButton > button {
        background-color: #ffffff !important;
        color: black !important;
        font-weight: bold !important;
        margin-top: 10px !important;
    }
    
    section[data-testid="stSidebar"] .stFormSubmitButton > button:hover {
        background-color: #fafafa !important;
        color: green !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def track_event(event_type: str, metadata: dict = None, count: int = 1):
    event = {
        "user_id": "anonymous",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "count": count,
        "metadata": metadata or {},
        "user_event": event_type,
    }
    try:
        supabase.table("user_events").insert([event]).execute()
    except Exception as e:
        error_message = str(e).lower()
        if "rate" in error_message or "limit" in error_message:
            print(f"SUPABASE RATE LIMIT: {e}")
        else:
            print(f"SUPABASE ERROR: {e}")


if "client_session_id" not in st.session_state:
    st.session_state.client_session_id = str(uuid.uuid4())
if "site_visit_tracked" not in st.session_state:
    st.session_state.site_visit_tracked = False
if not st.session_state.site_visit_tracked:
    try:
        track_event(
            "site_visit",
            metadata={
                "client_session_id": st.session_state.client_session_id,
            },
        )
    finally:
        st.session_state.site_visit_tracked = True


def send_file_to_backend(uploaded_file):
    files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
    response = requests.post("https://spotify-animation.fly.dev/process", files=files)

    return response


def send_zip_path_to_backend(zip_path: str, filename: str = "spotify_jsons.zip"):
    with open(zip_path, "rb") as f:
        files = {"file": (filename, f, "application/zip")}
        response = requests.post(
            "https://spotify-animation.fly.dev/process", files=files
        )
        return response


def send_image_request_to_backend(
    session_id, selected_attribute, analysis_metric, top_n, start_date, end_date
):
    data = {
        "session_id": session_id,
        "selected_attribute": selected_attribute,
        "analysis_metric": analysis_metric,
        "top_n": top_n,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    response = requests.post(
        "https://spotify-animation.fly.dev/generate_image", json=data
    )

    return response


def send_animation_request_to_backend(
    session_id,
    selected_attribute,
    analysis_metric,
    top_n,
    start_date,
    end_date,
    speed_for_bar_animation,
    days,
    interp_steps,
    period,
    figsize,
    dpi,
):
    data = {
        "session_id": session_id,
        "selected_attribute": selected_attribute,
        "analysis_metric": analysis_metric,
        "top_n": top_n,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "speed_for_bar_animation": speed_for_bar_animation,
        "days": days,
        "interp_steps": interp_steps,
        "period": period,
        "figsize": figsize,
        "dpi": dpi,
    }
    response = requests.post(
        "https://spotify-animation.fly.dev/generate_animation", json=data
    )

    return response


# Initialize session state with defaults
if "form_values" not in st.session_state:
    st.session_state.form_values = {
        "selected_attribute": "artist_name",
        "analysis_metric": "Streams",
        "speed_for_bar_animation": 60,
        "top_n": 5,
        "start_date": datetime(2023, 1, 1),
        "end_date": datetime.now(),
        "data_uploaded": False,
        "data_min_date": None,
        "data_max_date": None,
    }

# Initialize event tracking flags in session state
if "generate_image_clicked" not in st.session_state:
    st.session_state.generate_image_clicked = False

if "download_image_clicked" not in st.session_state:
    st.session_state.download_image_clicked = False

if "generate_animation_clicked" not in st.session_state:
    st.session_state.generate_animation_clicked = False

if "download_animation_clicked" not in st.session_state:
    st.session_state.download_animation_clicked = False

if "data_uploaded" not in st.session_state.form_values:
    st.session_state.form_values["data_uploaded"] = False

if "session_id" not in st.session_state:
    st.session_state.session_id = None


# Initialise date range in session state
if "data_min_date" not in st.session_state.form_values:
    st.session_state.form_values["data_min_date"] = None
if "data_max_date" not in st.session_state.form_values:
    st.session_state.form_values["data_max_date"] = None

with st.sidebar:
    st.markdown(
        """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h1 style="
        font-family: 'Fredoka', sans-serif;
        font-size: 26px;
        color: #fafafa;
        text-shadow: 0 0 40px #444444;
        text-align: center;
        letter-spacing: 1px;
        margin-bottom: 0.01em;
        margin-top: 1.2em;
        ">Display Options
    </h1>
    """,
        unsafe_allow_html=True,
    )
    with st.form(key="preferences_form"):
        selected_attribute = st.selectbox(
            "What data do you want to see?", ["Artists", "Songs", "Albums"]
        )

        if selected_attribute == "Artists":
            selected_attribute = "artist_name"
        elif selected_attribute == "Songs":
            selected_attribute = "track_name"
        else:
            selected_attribute = "album_name"

        analysis_metric = st.selectbox(
            "Streams or Time Listened? You Decide!",
            ["Number of Streams", "Time Listened"],
        )

        speed_of_visualization = st.selectbox(
            "How fast do you want the animation?", ["Slow", "Normal", "Fast"], index=1
        )

        if speed_of_visualization == "Slow":
            speed_for_bar_animation = 20
        elif speed_of_visualization == "Normal":
            speed_for_bar_animation = 28
        else:
            speed_for_bar_animation = 36

        top_n = st.slider(
            "How many items do you want to Display?",
            min_value=1,
            max_value=10,
            value=5,
        )

        col1, col2 = st.columns(2)
        with col1:
            min_date = (
                st.session_state.form_values["data_min_date"]
                if st.session_state.form_values["data_uploaded"]
                else st.session_state.form_values["start_date"]
            )
            start_date = st.date_input(
                "Select start date",
                value=st.session_state.form_values["start_date"],
                min_value=min_date,
                max_value=st.session_state.form_values[
                    "end_date"
                ],  # Prevent overlap with end_date
                key="start_date_input",
            )
            start_date = pd.to_datetime(start_date)
        with col2:
            max_date = (
                st.session_state.form_values["data_max_date"]
                if st.session_state.form_values["data_uploaded"]
                else st.session_state.form_values["end_date"]
            )
            end_date = st.date_input(
                "Select end date",
                value=st.session_state.form_values["end_date"],
                min_value=st.session_state.form_values[
                    "start_date"
                ],  # Prevent overlap with start_date
                max_value=max_date,
                key="end_date_input",
            )
            end_date = pd.to_datetime(end_date)

        submit_form = st.form_submit_button(
            label="Apply Preferences",
            use_container_width=True,
        )

    if submit_form:
        st.session_state.form_values.update(
            {
                "selected_attribute": selected_attribute,
                "analysis_metric": analysis_metric,
                "speed_for_bar_animation": speed_for_bar_animation,
                "top_n": top_n,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

# normalise user inputs
selected_attribute, analysis_metric = normalize_inputs(
    selected_attribute, analysis_metric
)

st.markdown(
    """
    <div style="text-align: center;
    margin-right: 0.8em;">
        <img src="data:image/svg+xml;base64,{}" width="200">
    </div>
    """.format(
        base64.b64encode(
            open(
                "assets/2024 Spotify Brand Assets/spotify_full_logo_black.svg", "rb"
            ).read()
        ).decode()
    ),
    unsafe_allow_html=True,
)

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h1 style='text-align: center;
               font-family: "Fredoka", sans-serif;
               font-size: 48px;
               background: linear-gradient(90deg, #1DB954, #1ED760, #00FFA3, #1DB954);
               -webkit-background-clip: text;
               -webkit-text-fill-color: transparent;
               letter-spacing: 2px;
               margin-bottom: -1.1em;
               margin-right: -0.5em;
               background-size: 300% 300%;'>
        VizTracks
    </h1>
    <style>
        @keyframes gradient-shift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h1 style='text-align: center;
               font-family: "Fredoka", sans-serif;
               font-size: 22px;
               color: #888888;
               letter-spacing: 1.5px;
               margin-top: 1em;
               margin-left: 0.3em;
               margin-bottom: 1em;'>
        Interactive visuals powered by your music
    </h1>
    """,
    unsafe_allow_html=True,
)


col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(
        """
    <div style="
        border: 1px solid #d3d3d3; 
        padding: 10px; 
        border-radius: 10px;
        text-align: center; 
        height: 75px;
        background: linear-gradient(to right, #f5f5f5, #fafafa);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        ">
        <div style="line-height: 1.2;">
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Upload Your
            </p>
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                History &nbsp;📥
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
    <div style="
        border: 1px solid #d3d3d3; 
        padding: 10px; 
        border-radius: 10px;
        text-align: center; 
        height: 75px;
        background: linear-gradient(to right, #f5f5f5, #fafafa);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        ">
        <div style="line-height: 1.2;">
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Customize Your
            </p>
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Preferences &nbsp;📊
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        """
    <div style="
        border: 1px solid #d3d3d3; 
        padding: 10px; 
        border-radius: 10px;
        text-align: center; 
        height: 75px;
        background: linear-gradient(to right, #f5f5f5, #fafafa);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        ">
        <div style="line-height: 1.2;">
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Generate
            </p>
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Visuals 🎞️
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col4:
    st.markdown(
        """
    <div style="
        border: 1px solid #d3d3d3; 
        padding: 10px; 
        border-radius: 10px;
        text-align: center; 
        height: 75px;
        background: linear-gradient(to right, #f5f5f5, #fafafa);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        ">
        <div style="line-height: 1.2;">
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                Download
            </p>
            <p style="font-size: 16px; font-weight: bold; margin: 0;">
                and Share! &nbsp;💾
            </p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )
st.markdown("<br>", unsafe_allow_html=True)

st.markdown(
    """
    <div style="text-align: center; color: #888888;">
        <p>Use the top left menu to set your preferences</p>
    </div>
""",
    unsafe_allow_html=True,
)

with st.expander("**Click here to see what my visualisations look like!**"):
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            "<h5 style='text-align: center;'>Bar Chart Race &nbsp;🎥</h5>",
            unsafe_allow_html=True,
        )
        st.video("./assets/visuals/artist_name_Streams_animation_final.mp4")

    with col2:
        st.markdown(
            "<h5 style='text-align: center;'>Bar Chart Image &nbsp;📸</h4>",
            unsafe_allow_html=True,
        )
        st.image("./assets/visuals/album_name_Streams_visual_max.jpg")

    st.markdown("<br>", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h2 style='font-family: "Fredoka", sans-serif; font-size: 26px; color: #888888; margin-bottom: -1.2em; margin-top: 0.5em;'>
        Load Your Music
    </h2>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "<hr style='border: 1px solid #1DB954; margin-top: 0.5em; margin-bottom: -0.2em;'>",
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom: -4.7em;'></div>", unsafe_allow_html=True)

with st.expander(
    "Click here for Data Upload Guide &nbsp; 📥",
):
    st.markdown(
        "In order to get your extended streaming history files, you must request your data from Spotify."
    )
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([0.5, 0.5])

    with col1:
        st.markdown(
            """
            **Step 1:** &nbsp; Open the [Privacy page](https://www.spotify.com/us/account/privacy/) on the Spotify website.
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            "**Step 2:** &nbsp; Scroll down to the 'Download your data' section."
        )
        st.markdown(
            "**Step 3:** &nbsp; Untick &nbsp;❌&nbsp; the Account Data section and tick &nbsp;✅&nbsp; the 'Extended Streaming History' section <br>",
            unsafe_allow_html=True,
        )
        st.markdown("**Step 4:** &nbsp; Click the 'Request data' button.")
        st.markdown(
            "**Step 5:** &nbsp; Spotify will then send you an email to confirm your request, which you need to confirm.",
            unsafe_allow_html=True,
        )
        st.markdown(
            "**Step 6:** &nbsp; After a few days, your data will be emailed. Download the Zip file and upload it below!",
            unsafe_allow_html=True,
        )

    with col2:
        st.image("./assets/visuals/download_guide.png", width=450)


uploaded_files = st.file_uploader(
    "Upload your Spotify data (ZIP or JSON files)",
    type=["zip", "json"],
    accept_multiple_files=True,
)

if uploaded_files and not st.session_state.form_values["data_uploaded"]:
    files = uploaded_files if isinstance(uploaded_files, list) else [uploaded_files]
    zip_files = [f for f in files if f.name.lower().endswith(".zip")]
    json_files = [f for f in files if f.name.lower().endswith(".json")]
    response = None

    if json_files:
        if zip_files:
            with st.spinner("Crunching your Spotify jams... Hold tight"):
                with tempfile.TemporaryDirectory() as td:
                    zip_path = os.path.join(td, "spotify_jsons.zip")
                    with zipfile.ZipFile(
                        zip_path, mode="w", compression=zipfile.ZIP_STORED
                    ) as zf:
                        for jf in json_files:
                            temp_json_path = os.path.join(td, jf.name)
                            os.makedirs(os.path.dirname(temp_json_path), exist_ok=True)
                            with open(temp_json_path, "wb") as out:
                                while True:
                                    chunk = jf.read(CHUNK_SIZE)
                                    if not chunk:
                                        break
                                    out.write(chunk)
                            try:
                                jf.seek(0)
                            except Exception:
                                pass
                            zf.write(temp_json_path, arcname=jf.name)
                            try:
                                os.remove(temp_json_path)
                            except Exception:
                                pass
                    response = send_zip_path_to_backend(zip_path)
    elif len(zip_files) == 1:
        with st.spinner("Crunching your Spotify jams... Hold tight"):
            response = send_file_to_backend(zip_files[0])
    elif len(zip_files) > 1:
        st.error("Please upload only one ZIP file, or upload the extracted JSON files.")
    else:
        st.error(
            "Please upload either a single ZIP file or the extracted files from Spotify."
        )
    if response is not None and response.status_code == 200:
        try:
            response_data = response.json()
            session_id = response_data["session_id"]
            start_date_file = pd.to_datetime(response_data["data_min_date"])
            end_date_file = pd.to_datetime(response_data["data_max_date"])

            st.session_state.session_id = session_id
            st.session_state.form_values.update(
                {
                    "start_date": start_date_file,
                    "end_date": end_date_file,
                    "data_min_date": start_date_file,
                    "data_max_date": end_date_file,
                    "data_uploaded": True,
                }
            )
            st.success(
                "History uploaded successfully! 🎉 Ready to visualize your jams 🎶"
            )
            st.rerun()
        except Exception as e:
            st.error(f"Error parsing response: {str(e)}")

    elif response is not None:
        st.error(
            "Failed to process file. Please make sure you uploaded the correct **ZIP file** from Spotify and not the extracted files."
        )
elif uploaded_files and st.session_state.form_values["data_uploaded"]:
    st.success("Upload successful! Ready to visualize your jams 🎶 🎉")

else:
    st.warning(
        "Please upload your Spotify ZIP file or the extracted JSON files to proceed."
    )
    df = None

selected_attribute, analysis_metric = normalize_inputs(
    st.session_state.form_values["selected_attribute"],
    st.session_state.form_values["analysis_metric"],
)


# Initialize session state for storing image bytes
if "bar_plot_bytes" not in st.session_state:
    st.session_state.bar_plot_bytes = None
if "file_name_for_download" not in st.session_state:
    st.session_state.file_name_for_download = None

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h2 style='font-family: "Fredoka", sans-serif; font-size: 26px; color: #888888; margin-bottom: -1.2em; margin-top: 0.5em;'>
        Generate Image
    </h2>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "<hr style='border: 1px solid #1DB954; margin-top: 0.5em; margin-bottom: -0.2em;'>",
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom: -4.7em;'></div>", unsafe_allow_html=True)
if st.button("Generate Image", key="generate_images_button"):
    st.session_state.generate_image_clicked = True

if st.session_state.generate_image_clicked:
    # Clear previous image result before storing new one
    st.session_state.bar_plot_bytes = None
    st.session_state.file_name_for_download = None

    track_event(
        "generate_image",
        metadata={
            "selected_attribute": selected_attribute,
            "analysis_metric": analysis_metric,
            "top_n": top_n,
        },
    )
    st.session_state.generate_image_clicked = False

    if hasattr(st.session_state, "session_id") and st.session_state.session_id:
        with st.spinner("Generating visual..."):
            response = send_image_request_to_backend(
                st.session_state.session_id,
                selected_attribute,
                analysis_metric,
                top_n,
                start_date,
                end_date,
            )

            if response.status_code == 200:
                try:
                    result = response.json()
                    image_base64 = result["image"]
                    filename = result["filename"]

                    # Convert base64 back to bytes
                    image_bytes = base64.b64decode(image_base64)

                    st.session_state.bar_plot_bytes = image_bytes
                    st.session_state.file_name_for_download = filename

                except Exception as e:
                    st.error(f"Error processing image response: {str(e)}")
            else:
                try:
                    error_data = response.json()
                    st.error(
                        f"Image generation failed: {error_data.get('error', 'Unknown error')}"
                    )
                except Exception:
                    st.error("Failed to generate image. Please try again.")
    else:
        st.warning("Please upload your Spotify JSON files to proceed.")


if st.session_state.bar_plot_bytes:
    st.markdown(
        "<h4 style='text-align: left;'>Bar Chart Image 📸</h4>",
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns([0.55, 0.44, 0.01])

    with col1:
        st.image(st.session_state.bar_plot_bytes)

    with col2:
        col_dl_1, col_dl_2, col_dl_3 = st.columns([0.005, 0.99, 0.005])
        with col_dl_2:
            st.write("Click the button below to download your visual:")
            clicked = st.download_button(
                label="Download Visual",
                data=st.session_state.bar_plot_bytes,
                file_name=st.session_state.file_name_for_download,
                mime="image/jpeg",
                key="download_bar_plot",
            )
            if clicked:
                st.session_state.download_image_clicked = True
                # Clear image from session state after download
                st.session_state.bar_plot_bytes = None
                st.session_state.file_name_for_download = None

            st.markdown(
                "<p style='text-align: left; font-size: 14px; color: gray;'>Then you can upload it to social media and show friends your superior music taste!</p>",
                unsafe_allow_html=True,
            )

if st.session_state.download_image_clicked:
    track_event(
        "download_image",
        metadata={
            "selected_attribute": selected_attribute,
            "analysis_metric": analysis_metric,
            "top_n": top_n,
        },
    )
    st.session_state.download_image_clicked = False


class AnimationState:
    def __init__(self, top_n):
        self.prev_interp_positions = self.prev_positions = (
            self.current_new_positions
        ) = list(range(9, 9 - top_n, -1))
        self.prev_names = [""] * top_n
        self.prev_widths = [0] * top_n


animation_state = AnimationState(top_n)

if "temp_file_path_bar_anim" not in st.session_state:
    st.session_state.temp_file_path_bar_anim = None  # Initialize state

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h2 style='font-family: "Fredoka", sans-serif; font-size: 26px; color: #888888; margin-bottom: -1.2em; margin-top: 0.5em;'>
        Generate Animation
    </h2>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "<hr style='border: 1px solid #1DB954; margin-top: 0.5em; margin-bottom: -0.2em;'>",
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom: -4.7em;'></div>", unsafe_allow_html=True)
# Helper: Only allow one animation process at a time per session
if "animation_job_id" not in st.session_state:
    st.session_state.animation_job_id = None
if "animation_processing" not in st.session_state:
    st.session_state.animation_processing = False

if st.button("Generate Animation", key="generate_animation_button"):
    # Only allow one animation process at a time
    if st.session_state.animation_processing:
        st.warning(
            "An animation is already being generated. Please wait for it to finish."
        )
    else:
        # Clear previous animation result before storing new one
        if st.session_state.temp_file_path_bar_anim:
            try:
                import os

                os.remove(st.session_state.temp_file_path_bar_anim)
            except Exception:
                pass
        st.session_state.temp_file_path_bar_anim = None
        st.session_state.file_name_for_download = None
        st.session_state.download_animation_clicked = False
        st.session_state.animation_processing = True
        st.session_state.animation_job_id = str(
            uuid.uuid4()
        )  # Unique job ID for this request
        track_event(
            "generate_animation",
            metadata={
                "selected_attribute": selected_attribute,
                "analysis_metric": analysis_metric,
                "top_n": top_n,
                "job_id": st.session_state.animation_job_id,
            },
        )

        if hasattr(st.session_state, "session_id") and st.session_state.session_id:
            with st.spinner("Generating animation..."):
                message_placeholder = st.empty()
                message_placeholder.write(
                    "Hold tight, this may take a few minutes if your data covers many years 😬"
                )
                response = send_animation_request_to_backend(
                    st.session_state.session_id,
                    selected_attribute,
                    analysis_metric,
                    top_n,
                    start_date,
                    end_date,
                    speed_for_bar_animation,
                    days,
                    interp_steps,
                    period,
                    figsize,
                    dpi,
                )
                if response.status_code == 200:
                    try:
                        result = response.json()
                        video_base64 = result["video"]
                        filename = result["filename"]
                        video_bytes = base64.b64decode(video_base64)
                        with tempfile.NamedTemporaryFile(
                            delete=False, suffix=".mp4"
                        ) as temp_file:
                            temp_file.write(video_bytes)
                            temp_file_path = temp_file.name
                            st.session_state.temp_file_path_bar_anim = temp_file_path
                            st.session_state.file_name_for_download = filename
                        message_placeholder.empty()
                    except Exception as e:
                        st.session_state.temp_file_path_bar_anim = None
                        st.session_state.file_name_for_download = None
                        st.error(f"Error processing animation response: {str(e)}")
                else:
                    try:
                        error_data = response.json()
                        st.session_state.temp_file_path_bar_anim = None
                        st.session_state.file_name_for_download = None
                        st.error(
                            f"Animation generation failed: {error_data.get('error', 'Unknown error')}"
                        )
                    except Exception:
                        st.session_state.temp_file_path_bar_anim = None
                        st.session_state.file_name_for_download = None
                        st.error("Failed to generate animation. Please try again.")
                st.session_state.animation_processing = False
        else:
            st.session_state.animation_processing = False
            st.warning("Please upload your Spotify JSON files to proceed.")

if st.session_state.get("temp_file_path_bar_anim"):
    st.markdown(
        "<h4 style='text-align: left;'>Bar Chart Race 📊</h4>",
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns([0.55, 0.44, 0.01])
    with col1:
        st.video(st.session_state.temp_file_path_bar_anim)

    with col2:
        col1, col2, col3 = st.columns([0.005, 0.99, 0.005])
        with col2:
            st.write("Click the button below to download your Animation:")
            with open(st.session_state.temp_file_path_bar_anim, "rb") as f:
                clicked = st.download_button(
                    label="Download Animation",
                    data=f.read(),
                    file_name=f"{selected_attribute}_{analysis_metric}_animation.mp4",
                    mime="video/mp4",
                    key="download_bar_animation",
                )
                if clicked:
                    st.session_state.download_animation_clicked = True
                    # Clear animation from session state after download
                    try:
                        import os

                        os.remove(st.session_state.temp_file_path_bar_anim)
                    except Exception:
                        pass
                    st.session_state.temp_file_path_bar_anim = None
                    st.session_state.file_name_for_download = None

if st.session_state.download_animation_clicked:
    track_event(
        "download_animation",
        metadata={
            "selected_attribute": selected_attribute,
            "analysis_metric": analysis_metric,
            "top_n": top_n,
        },
    )
    st.session_state.download_animation_clicked = False  # reset flag

st.markdown("<br>", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@700&display=swap" rel="stylesheet">
    <h2 style='font-family: "Fredoka", sans-serif; font-size: 26px; color: #888888; margin-bottom: 0.5em;'>
        License
    </h2>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "<hr style='border: 1px solid #1DB954; margin-top: -1.2em; margin-bottom: 1em;'>",
    unsafe_allow_html=True,
)
st.markdown(
    """
    This application is open-source and available under the GNU General Public License v3.

    For more details, visit the [GitHub repository](https://github.com/maxwellbernard/VizTracks). 
    """,
    unsafe_allow_html=True,
)
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

st.markdown(
    """
    <div style="text-align: center;">
        <a href="https://github.com/maxwellbernard" target="_blank" style="font-size: 16px; 
          font-weight: bold; color: #1db954; text-decoration: none;">
            Visit my GitHub for more projects and code! 🚀
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div style="text-align: center; font-size: 15px;">
        Connect with me on <a href="https://linkedin.com/in/maxwell-bernard" target="_blank" style="color: #0a66c2; text-decoration: underline;">LinkedIn</a> 🔗
    </div>
    """,
    unsafe_allow_html=True,
)

# feedback button
st.markdown(
    """
    <style>
    .feedback-btn {
        font-size: 14px; 
        color: #222 !important; 
        text-decoration: none !important;
        background-color: #f0f0f0; 
        padding: 10px 25px; 
        border-radius: 5px;
        transition: color 0.2s;
        display: inline-block;
        border: 1px solid #ccc;
    }
    .feedback-btn:hover {
        color: #1DB954 !important;
        border: 1px solid #1DB954;
    }
    </style>
    <div style="text-align: center; margin-top: 50px;">
        <a href="https://docs.google.com/forms/d/e/1FAIpQLSepkDYlPQ4AcXyAMPSAYtUcktg0ui2GSsKKL5i58asGbVUO9w/viewform?usp=header" target="_blank" 
           class="feedback-btn">
            Give Feedback &nbsp;✍️
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)

footer = """
<style>
.footer {
    width: 100%;
    color: #b0b0b0;
    text-align: center;
    font-size: 11px;
    padding: 8px 0;
    font-family: Arial, sans-serif;
    margin-top: 70px;
}
</style>
<div class="footer">
    Disclaimer: Spotify is a registered trademark of Spotify AB. This app is a third-party tool that uses Spotify data and is not affiliated with or endorsed by Spotify.
</div>
"""
st.markdown(footer, unsafe_allow_html=True)
