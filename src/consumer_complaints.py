from collections import Counter
from datetime import datetime
import csv
import logging
import sys

logging.basicConfig(filename='complaints_output.log', level=logging.INFO)

MANDATORY_HEADERS = ['Date received', 'Product', 'Company']


def run(input_file_name, output_file_name):
    """
    Main root function to start the run
    :param input_file_name: name of file to read from
    :type input_file_name: str
    :param output_file_name: name of file to write to
    :type output_file_name: str
    """
    csv_gen = csv_generator(input_file_name)
    results = parse_csv(csv_gen)
    publish_results(results, output_file_name)


def csv_generator(input_file_name):
    """
    I/O for reading csv and yielding each row
    :param input_file_name: name of file to read from
    :type input_file_name: str
    """
    with open(input_file_name) as f:
        row_gen = csv.reader(f)
        for row in row_gen:
            yield row
        f.close()


def parse_csv(row_generator):
    """
    Parses through the data to construct the results dict
    :param row_generator: generator that yields 1 row
    :type row_generator: generator
    :return: dict containing aggregated counts for company complaints grouped by product
    and year
    :rtype: dict
    """
    results = {}
    headers = next(row_generator)
    date_pointer, product_pointer, company_pointer = get_header_pointers(headers)
    row_count = 0
    # all rows need this many cols
    max_cols_count = max([date_pointer, product_pointer, company_pointer]) + 1

    logging.info("Starting to parse CSV")

    for row in row_generator:
        if len(row) < max_cols_count:
            raise ValueError("Row %s does not have enough columns: %s" % (row_count, ", ".join(row)))
        complaint_date = row[date_pointer]
        product_name = row[product_pointer]
        company_name = row[company_pointer]

        year = extract_year(complaint_date, row_count)
        clean_product = clean_str(product_name)
        clean_company = clean_str(company_name)
        row_parsing(clean_product, year, clean_company, results)
        row_count += 1
        if row_count % 1000 == 0:
            logging.info("Parsed row %s" % row_count)

    logging.info("Successfully parsed CSV data")
    return results


def get_header_pointers(headers):
    """
    Searches for where the needed column names are in case they aren't in the same order
    :param headers: first row of csv for column names
    :type headers: list[str]
    :return: integers showing where in the row list the date,
    product, and company pointers are
    :rtype: int, int, int
    """
    try:
        date_pointer = headers.index(MANDATORY_HEADERS[0])
        product_pointer = headers.index(MANDATORY_HEADERS[1])
        company_pointer = headers.index(MANDATORY_HEADERS[2])
    except ValueError:
        raise ValueError('Mandatory Header Not Found')

    return date_pointer, product_pointer, company_pointer


def extract_year(complaint_date, row_count):
    """
    Extracts the year value from a date string
    :param complaint_date: date of the complaint
    :type complaint_date: str
    :param row_count: what row the count is on
    :type row_count: int
    :return: the year
    :rtype: int
    """
    try:
        dt = datetime.strptime(complaint_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Row %s doesn't have proper date format: %s" % (row_count, complaint_date))
    return dt.year


def clean_str(string):
    """
    lower cases string and removed all extra spaces at end and begining of str
    :param string: input str
    :type string: str
    :return: cleaned str
    :rtype: str
    """
    return string.lower().strip()


def row_parsing(clean_product, year, clean_company, results):
    """
    Uses data from a row to count data for results, in place
    :param clean_product: name of product for the row
    :type clean_product: str
    :param year: year for row
    :type year: int
    :param clean_company: name of company for complaint
    :type clean_company: str
    :param results: where the aggregated data is stored
    :type results: dict
    """
    if clean_product not in results:
        results[clean_product] = {year: Counter([clean_company])}
    elif year not in results[clean_product]:
        results[clean_product][year] = Counter([clean_company])
    else:
        results[clean_product][year][clean_company] += 1


def publish_results(results, output_file_name):
    """
    I/O, saves results dict to a csv file
    :param results: stores the aggregated data
    :type results: dict
    :param output_file_name: name of the out file to save to
    :type output_file_name: str
    """
    logging.info("Saving results to %s" % output_file_name)
    with open(output_file_name, 'w') as out_f:
        for product_name_key in sorted(results.keys()):
            for year_key in sorted(results[product_name_key].keys()):
                row = construct_row(results, product_name_key, year_key)
                out_f.write(",".join(row))
                out_f.write("\n")
        out_f.close()
    logging.info("Results successfully saved to %s" % output_file_name)


def construct_row(results, product, year):
    """
    Constructs the rows for the output csv
    :param results: aggregated data, created by parsing complaints input csv
    :type results: dict
    :param product: products that complaints are grouped by
    :type product: str
    :param year: year that complaints are grouped by
    :type year: int
    :return: 1 row of aggregated information
    :rtype: list[str]
    """
    if "," in product:
        clean_product = '"%s"' % product
    else:
        clean_product = product

    row = [clean_product, str(year)]
    company_counter = results[product][year]
    total = total_complaints(company_counter)
    row.append(str(total))
    row.append(str(total_companies(company_counter)))
    row.append(str(highest_percentage(company_counter, total)))
    return row


def total_complaints(company_counter):
    """
    Sums the number of complaints in the company counter
    :param company_counter: counter counting complaints per company
    :type company_counter: Counter
    :return: number of complains for this particular year and product
    :rtype: int
    """
    total = 0
    for k, v in company_counter.items():
        total += v
    return total


def total_companies(company_counter):
    """
    Counts the number of companies that had at least 1 complaint
    :param company_counter: counting of complaints per company
    :type company_counter: Counter
    :return: number of companies that had at least 1 complain
    :rtype: int
    """
    return len(company_counter.keys())


def highest_percentage(company_counter, total):
    """
    Calculates the percentage of the highest company
    complaints for this year and product
    :param company_counter: counter of complaints per company
    :type company_counter: Counter
    :param total: total number of complaints for this year and product
    :type total: int
    :return: percentage rounded to near integer and ranging from 1 to 100
    :rtype: int
    """
    top_count = company_counter.most_common(1)[0][1]
    percentage = top_count/round(float(total))*100
    return int(round(percentage))


if __name__ == "__main__":
    if len(sys.argv) == 3:
        print('Arguments: ', sys.argv)
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        run(input_file, output_file)
    else:
        ValueError("Wrong number of arguments, "
                   "pass the input file path and output file path")
