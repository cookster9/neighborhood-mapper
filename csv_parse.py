import csv
import creds

csv_path = creds.downloads_folder+'TXT/NAD_r11.txt'

# Open input and output files
with open(csv_path, 'r') as input_file, open('output.csv', 'w', newline='') as output_file:
    # Create CSV reader and writer objects
    reader = csv.reader(input_file)
    writer = csv.writer(output_file)

    # Iterate over rows in input file and write to output file
    for row in reader:
        if(row[1].upper() == 'TN' and row[2].upper() == 'DAVIDSON'):
            writer.writerow(row)