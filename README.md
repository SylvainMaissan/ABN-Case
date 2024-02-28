# ABN-Case: KommatiPara Data Cleaning Project

This project contains the code and data for a data cleaning exercise performed by KommatiPara, a fictional cryptocurrency trading company. The goal of the project was to create a dataset containing the email addresses of clients from the United Kingdom and the Netherlands, along with some of their financial information.

The code in this repository is a simplified version of the code used in the actual project. In the real project, the code was stored in a separate repository, and the data was stored in a separate database. For the purposes of this exercise, the data and code have been combined into a single repository.

The code in this repository performs the following steps:
1. Reads in two datasets: one containing client information, and another containing financial information.
2. Cleans the client dataset by filtering out rows that contain a country other than the United Kingdom or the Netherlands.
3. Joins the two datasets on the client ID, and drops the client ID column.
4. Renames some of the columns in the resulting dataset.
5. Writes the resulting dataset to a CSV file.

The data in this repository consists of two CSV files: one containing client information, and another containing financial information. The client information dataset contains the following columns:
- id
- first_name
- last_name
- email
- country

The financial information dataset contains the following columns:
- id
- bitcoin_address (btc_a)
- credit_card_type (cc_t)
- credit_card_number (cc_n)

The code and data in this repository are provided for educational purposes only. They are not intended to be used in a production environment.