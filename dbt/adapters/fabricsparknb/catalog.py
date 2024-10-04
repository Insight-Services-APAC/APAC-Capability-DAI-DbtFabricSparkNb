import io
import json
import agate


@staticmethod
def ListRelations(profile):
    # Open the file
    with io.open(profile.project_root + '/metaextracts/ListRelations.json', 'r') as file:
        # Load JSON data from file
        data = json.load(file)

    table = agate.Table.from_object(data)

    return table

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
        #row.update(parsed_data)
        #row.pop('information')
    # Convert the data to an Agate table
    


@staticmethod
def GetColumnsInRelation(profile, schema, identifier):
    # Open the file
    with io.open(profile.project_root + '/metaextracts/DescribeRelations.json', 'r') as file:
        # Load JSON data from file
        data = json.load(file)
    
    #transforming Database/schema name and table name  to lower case
    schema = schema.lower()
    identifier = identifier.lower()
    
    table = agate.Table.from_object(data)    

    # Filter the table
    filtered_table = table.where(lambda row: row['namespace'] == schema and row['tableName'] == identifier)

    return filtered_table


@staticmethod
def ListSchemas(profile):
    # Open the file
    with io.open(profile.project_root + '/metaextracts/ListSchemas.json', 'r') as file:
        # Load JSON data from file
        data = json.load(file)
   
    table = agate.Table.from_object(data)    

    return table


def ListSchema(profile, schema):
    # Open the file
    with io.open(profile.project_root + '/metaextracts/ListSchemas.json', 'r') as file:
        # Load JSON data from file
        data = json.load(file)
    
    table = agate.Table.from_object(data)    

    #transforming Database/schema name to lower case
    schema = schema.lower()   
    
    # Filter the table
    filtered_table = table.where(lambda row: str(row['namespace']).lower() == schema)

    return filtered_table
