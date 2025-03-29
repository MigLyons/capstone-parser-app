import json
import os
import re
import requests
from urllib.request import urlretrieve  # For downloading files from URLs
import azure.functions as func
import logging
from msal import ConfidentialClientApplication
import pymupdf  # PyMuPDF for PDF processing

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="profilecreatedormodifiedqueue",
                               connection="capstoneservicebus_SERVICEBUS") 
@app.blob_output(arg_name="OutputToBlob", path="profiles/{datetime.Now}.json", connection="BlobStorageConnectionString")
def ProfileCreatedOrModified(azservicebus: func.ServiceBusMessage, OutputToBlob: func.Out[str]):
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
    url = azservicebus.get_body().decode()
    filePath = _sharepointQuery(_getAccessToken(), url)
    _get_file_type(filePath)
    content = _read_pdf_with_metadata(filePath)
    if not content:
        print("Failed to extract content from the file.")
        return
    profile_data = _parse_profile(content)
    profile_json = json.dumps(profile_data, indent=2)
    OutputToBlob.set(profile_json)

def _getAccessToken():
    client = ConfidentialClientApplication(
        client_id=os.environ["clientId"],
        client_credential=os.environ["clientSecret"],
        authority=os.environ["authorityURL"]
    )
    token_result = client.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    access_token = 'Bearer ' + token_result["access_token"]
    return access_token

def _sharepointQuery(access_token, url):
    # Make a GET request to the SharePoint REST API to retrieve the file from the event
    fileData = requests.get(url,
        headers={
            "Authorization": access_token,
        }, ).json()
    filePath, fileResponse = urlretrieve(fileData['@microsoft.graph.downloadUrl'], "tmp/tempProfile.pptx")
    return filePath

REQUIRED_SECTIONS = [
    "Name",
    "Email",
    "Job Title",
    "Executive Summary",
    "Technical Expertise",
    "Functional Expertise",
    "Experience",
    "Mobility",
    "Industry Sectors",
    "Languages Spoken",
    "Certifications",
    "Methodologies"
]

BULLET_SECTIONS = [
    "Technical Expertise",
    "Functional Expertise",
    "Industry Sectors",
    "Languages Spoken",
    "Certifications",
    "Methodologies"
]

LONGFORM_SECTIONS = [
    "Executive Summary",
    "Mobility",
    "Name",
    "Email",
    "Job Title"
]

def _get_file_type(file_path):
    """Determines if the uploaded file is a PDF or another type."""
    if file_path.endswith('.pdf'):
        return 'pdf'
    else:
        return 'unsupported'

def _read_pdf_with_metadata(file_path):
    """Reads text and formatting metadata from a PDF file using PyMuPDF."""
    try:
        doc = pymupdf.open(file_path)
        content = []
        current_section = None
        for page in doc:
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        for span in line["spans"]:
                            text_val = span["text"].strip()
                            if text_val in REQUIRED_SECTIONS:
                                #If the text is a section title, switch the section but do not add the value to the content
                                current_section = text_val
                            else:
                                content.append({
                                    "section": current_section,
                                    "text": text_val
                                })
        return content
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return None

def _extract_contact_information(content):

    """Extracts contact information using regex."""

    header_pattern = r'[A-Z]\.\s[A-Za-z]+(?:\s[A-Za-z]+)?\s[-–—]\s["“”](.+?)["“”]'
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    header = None
    email = None
    for item in content:
        if re.match(header_pattern, item["text"]):
            header = item["text"]
            content.remove(item)
        elif re.search(email_pattern, item["text"]):
            email = re.search(email_pattern, item["text"]).group()
            content.remove(item)
    header = re.split(r'\s*[-–—]\s*', header) if header else None
    job_title = re.sub(r"[\"“”]", "", header[1] if header else None)
    contact_info = {
        "name": header[0] if header else None,
        "email": email if email else None,
        "job_title": job_title if job_title else None
    }
    return contact_info

def _parse_profile(content):
    """
    Parses the extracted content into a structure that fits the JSON format:
    {
      "sharePointRef": null,
      "sections": [
          {
             "section_name": "SectionName",
             "section_content": "content" or [ ... ]
          },
          ...
      ]
    }
    """
    # Initialize a map to hold text for each section.
    # For Experience, use a list; for others, use a string.
    sections_map = {section: [] for section in REQUIRED_SECTIONS}
    # handle name, email, and job title separately via regex
    contact_sections = _extract_contact_information(content)

    current_section = None
    for item in content:
        text = item["text"].strip()
        current_section = item["section"].strip() if item["section"] else current_section
        if current_section and text:
            if current_section in sections_map:
                if sections_map[current_section]:
                    sections_map[current_section].append(text)
                else:
                    sections_map[current_section] = [text]

    sections_map["Name"] = contact_sections["name"]
    sections_map["Email"] = contact_sections["email"]
    sections_map["Job Title"] = contact_sections["job_title"]
    # Process each section using helper functions.
    for section in REQUIRED_SECTIONS:
        if not sections_map[section]:
            continue
        # if section in BULLET_SECTIONS:
        #     sections_map[section] = _bullet_section_helper(sections_map[section])
        # if section in LONGFORM_SECTIONS:
        #     sections_map[section] = _longform_section_helper(sections_map[section])
        if section == "Experience":
            sections_map[section] = _experience_section_helper(sections_map[section])

    # Build a list of sections with keys "section_name" and "section_content".
    content_list = []
    for section in sections_map:
        if sections_map[section]:
            if section in LONGFORM_SECTIONS:
                content_list.append({
                    "section_name": section,
                    "section_content": sections_map[section]
                })
            else:
                for val in sections_map[section]:
                    if val:
                        content_list.append({
                            "section_name": section,
                            "section_content": val
                        })
    
    return {
        "sharePointRef": None,
        "sections": content_list
    }

def _bullet_section_helper(section_text):
    """
    Splits a string on bullet markers '•' into a list and removes any bullet characters.
    """
    bullets = section_text.split("•")
    bullets = [bullet.strip() for bullet in bullets if bullet.strip()]
    return bullets

def _longform_section_helper(section_text):
    """
    Removes bullet markers for longform sections.
    """
    return re.sub(r"•\s*", "", section_text).strip()

def _experience_section_helper(lines):
    """
    Processes the Experience section. Filters out empty entries and removes bullet characters.
    """
    projects = []
    project_info = []
    project_details = []
    project_contents = None
    for line in lines:

        if re.match(r'^[A-Z][a-z]*(?:\s+\w+)*\s[-–—]\s\w+(?:\s\w+)*\s[-–—]\s\w+(?:\s\w+)*', line):
            if project_contents:
                processed_project = project_contents
                project_details.append(processed_project)
                project_contents = []
            project_info.append(_experienceHeaderHelper(line))
        else:
            if project_contents:
                project_contents.append(line)
            else:
                project_contents = [line]
    else:
        processed_project = project_contents
        project_details.append(processed_project)
    for info, details in zip(project_info, project_details):
        projects.append({
            "project_header": info,
            "project_details": details
        })
    return projects

def _experienceHeaderHelper(line):
    """
    Processes the Experience section header. Removes bullet characters and splits the line into components.
    """
    line = re.sub(r"•\s*", "", line).strip()
    parts = re.split(r'\s[-–—]\s', line)
    sections = [part.strip() for part in parts if part.strip()]
    return {
        "project_title": sections[0] if sections[0] else None,
        "project_position": sections[1] if sections[1] else None,
        "project_industry": sections[2] if sections[2] else None
    }
