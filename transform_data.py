import sqlite3
import requests

def transform_data(data):
    transformed_data = []
    for user in data:
        transformed_user = {
            'name': user['name'],
            'email': user['email'],
            'phone': user['phone']
        }

        # Extracting city and zipcode from address
        address = user['address']
        transformed_user['city'] = address['city']
        transformed_user['zipcode'] = address['zipcode']

        # Calculating the total length of the 'name' and 'email' fields
        transformed_user['name_length'] = len(user['name'])
        transformed_user['email_length'] = len(user['email'])

        # Applying a conditional transformation based on the length of the name
        if len(user['name']) > 15:
            transformed_user['name_category'] = 'Long Name'
        else:
            transformed_user['name_category'] = 'Short Name'

        transformed_data.append(transformed_user)

    return transformed_data

def fetch_data_from_api():
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error fetching data. Status code: {response.status_code}')
        return None
    
def insert_data_into_db(data):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS users (name TEXT, email TEXT, phone TEXT, city TEXT, zipcode TEXT, name_length INTEGER, email_length INTEGER, name_category TEXT)')


    for user in data:
        cursor.execute('INSERT INTO users (name, email, phone, city, zipcode, name_length, email_length, name_category) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                       (user['name'], user['email'], user['phone'], user['city'], user['zipcode'], user['name_length'], user['email_length'], user['name_category']))

    conn.commit()
    conn.close()



if __name__ == "__main__":
    data = fetch_data_from_api()
    transformed_data = transform_data(data)
    print(transformed_data)
    insert_data_into_db(transformed_data)
