import json
import agate

# Open the file
with open('catalog.json', 'r') as file:
    # Load JSON data from file
    data = json.load(file)

for row in data:
    # Access the 'information' field
    information = row.get('information', None)

    # Split the string into lines
    lines = information.splitlines()
    #print(len(lines))
   
    # Initialize an empty dictionary to hold the parsed data
    parsed_data = {}

    # Iterate over each line
    for line in lines:
        #print(line)
        # Split the line into key and value
        pairs = line.split(': ')
        key = pairs[0]
        if len(pairs) > 1:
            value = line.split(': ')[1]
        else:
            value = None
    
        # Add the key-value pair to the dictionary
        parsed_data[key] = value
    #print(parsed_data)
    # Add the parsed data to each row    
    row.update(parsed_data)
    #row.pop('information')
# Convert the data to an Agate table
table = agate.Table.from_object(data)
print(table)