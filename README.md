# Thang Tran's Coding Submission

# How to Run

As specified, there are no external dependancies for this project (nothing that needs to be pip installed).  You can run the parser by running the command at the root folder:

`python3.8 ./src/consumer_complaints.py ./input/complaints.csv ./output/report.csv`

or the run.sh file that also just runs the same command.

Python 3.8 was used.

# While running

Information for the run is logged at complaints_output.log.  

# How the code works?

The code is very simple.  One part of the code reads the input csv file line by line and spits those lines to a parser that uses the data in each row or line to be stored in a nested dictionary/hashmap named `results`.  So the data is read and aggregated 1 row by row and avoiding the likelyhood of storing too much memory.

The results dictionary structure consists of:
    {Keys for all products : 
        {Keys for years for each product: 
            {Keys for companies: integer that's the count for each complaint for that company, product, and year}}}

After the csv input file is successfully parsed, the results dictionary aggregated data is further aggregated data to the 2 counts and 1 percentage.  For each Product, Year, there's a count for total number of complaints, total number of companies complained about, percentage for the company that received the most number of complaints.

This is very similar to a SQL GROUP BY on the product, year with a SUM(*), SUM(DISTINCT company), MAX(company_complaints_pct).

The script requires the data contain 'Date received', 'Product', 'Company' headers and for each row to have the data for those columns in every record. The order of the columns does matter as long as it matches the headers.

Right now, the code assumes the csv file to be mostly cleaned as in the correct csv formatting and correct data formatting especially the date column and the strategy is to fail when encountering uncleaned data instead of just skipping or salvaging the data.

# Things that could be improved

1. Modes for different approaches when encountering bad dirty data: raising exception, providing some solution, or ignoring bad dirty data
2. More verbose logging
3. Storing the results into a NoSQL database since the results data could break RAM if the data is large enough 
and number of companies, products, and years exceeds millions.  (This is not the same as number of rows)
There could be millions of products, millions of companies, and obviously up to 100 years.
