#!/usr/bin/python

from bs4 import BeautifulSoup

# Read the HTML file into a string
file_path = 'C:\\Users\\Vijay\\OneDrive\\Desktop\\html_file.txt'
with open(file_path, 'r', encoding='utf-8') as file:
    html_content = file.read()

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

#  Extract content of the <title> tag
title = soup.title.string


#  Prints extracted title
print("Title:", title)

