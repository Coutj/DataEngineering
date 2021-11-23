from faker import Faker
import csv

output = open('myCSV.CSV', mode='w')
fake = Faker()

my_writer = csv.writer(output)
header = ['name', 'age', 'street', 'city', 'state', 'zip', 'lng', 'lat']
my_writer.writerow(header)

for index in range(1000):
    my_writer.writerow(
        [
            fake.name(),
            fake.random_int(min=18, max=80, step=1),
            fake.street_address(),
            fake.city(),
            fake.state(),
            fake.zipcode(),
            fake.longitude(),
            fake.latitude()
        ]
    )

output.close()
