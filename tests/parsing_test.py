import unittest
#import azure.functions as func
from function_app import _get_file_type, _parse_profile, _read_pdf_with_metadata, _extract_contact_information, _experience_section_helper, _experienceHeaderHelper

class TestParser(unittest.TestCase):

    def test_get_file_type_pptx_success(self):
        #Test with a .pptx file to ensure it returns sucesss
        file_path = "test_files/test.pptx"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'Powerpoint')

    def test_get_file_type_altFileType_failure(self):
        #Test with an unsupported file type to ensure it returns failure
        file_path = "test_files/test.txt"
        result = _get_file_type(file_path)
        self.assertEqual(result, 'unsupported')

    def test_read_pdf_with_metadata_success(self):
        #Test reading a PDF file with metadata to ensure it returns content correctly
        file_path = "test_assets/OrrProfile.pptx"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)
    
    def test_read_pdf_with_metadata_failure(self):
        #Test reading a non-existent PDF file to ensure it handles errors gracefully
        file_path = "test_assets/non_existent_file.pptx"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNone(result)

    def test_read_pdf_with_metadata_blankpresentation_failure(self):
        #Test reading a blank PowerPoint file to ensure it handles empty content gracefully
        file_path = "test_assets/BlankPresentation.pptx"
        result = _read_pdf_with_metadata(file_path)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 0)

    def test_extract_contact_information_success(self):
        content = [{'section': None, 'text': 'M. Gallegos Lyons - "Software Engineer"'},
                    {'section': 'Executive Summary', 'text': 'This is an executive summary. M Gallegos Lyons engineers software,'},
                    {'section': 'Executive Summary', 'text': 'develops solutions, units the tests, and qualities the assurance.'},
                    {'section': 'Experience', 'text': 'Marketing - Copy writer - Technology'},
                    {'section': 'Experience', 'text': 'wrote copy for social media messages'},
                    {'section': 'Experience', 'text': 'promoted webinars and whitepapers'},
                    {'section': 'Experience', 'text': 'Email madeleinel@cognizant.com'},
                    {'section': 'Experience', 'text': 'Montana (Mountain Time)'},
                    {'section': 'Functional Expertise', 'text': 'Minor car repair'},
                    {'section': 'Technical Expertise', 'text': 'Salesforce Lightning'},
                    {'section': 'Certifications', 'text': 'National Latin Exam silver medal'},
                    {'section': 'Industry Sectors', 'text': 'Academia'},
                    {'section': 'Methodologies', 'text': 'Agile'},
                    {'section': 'Languages Spoken', 'text': 'English'},
                    {'section': 'Mobility', 'text': 'No'}]
        result = _extract_contact_information(content)
        result_keys = list(result.keys())
        self.assertIsNotNone(result)
        self.assertEqual(len(result_keys), 3)        

    def test_extract_contact_information_failure(self):
        #Test with empty content to ensure it handles no data gracefully
        content = None
        result = _extract_contact_information(content)
        self.assertIsNone(result)

    def test_parse_profile_success(self):
        content = [{'section': None, 'text': 'M. Gallegos Lyons - "Software Engineer"'},
                    {'section': 'Executive Summary', 'text': 'This is an executive summary. M Gallegos Lyons engineers software,'},
                    {'section': 'Executive Summary', 'text': 'develops solutions, units the tests, and qualities the assurance.'},
                    {'section': 'Experience', 'text': 'Marketing - Copy writer - Technology'},
                    {'section': 'Experience', 'text': 'wrote copy for social media messages'},
                    {'section': 'Experience', 'text': 'promoted webinars and whitepapers'},
                    {'section': 'Experience', 'text': 'Email madeleinel@cognizant.com'},
                    {'section': 'Experience', 'text': 'Montana (Mountain Time)'},
                    {'section': 'Functional Expertise', 'text': 'Minor car repair'},
                    {'section': 'Technical Expertise', 'text': 'Salesforce Lightning'},
                    {'section': 'Certifications', 'text': 'National Latin Exam silver medal'},
                    {'section': 'Industry Sectors', 'text': 'Academia'},
                    {'section': 'Methodologies', 'text': 'Agile'},
                    {'section': 'Languages Spoken', 'text': 'English'},
                    {'section': 'Mobility', 'text': 'No'}]
        result = _parse_profile(content)
        result_keys = list(result.keys())
        self.assertIsNotNone(result)
        self.assertTrue(len(result) > 0)
        self.assertIn('sharePointRef', result_keys)

    def test_parse_profile_failure(self):
        #Test with empty content to ensure it handles no data gracefully
        content = None
        result = _parse_profile(content)
        self.assertIsNone(result)

    def test_experience_header_helper(self):
        #Test that the experience header is correctly identified in the content
        content = "Apex Refactoring - Developer - Business"
        result = _experienceHeaderHelper(content)
        self.assertIsNotNone(result)
