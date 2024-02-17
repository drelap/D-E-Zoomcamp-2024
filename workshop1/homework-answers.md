# **Homework**: Data talks club data engineering zoomcamp Data loading workshop



# 1. Use a generator

Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

**Answer the following questions:**

- **Question 1: What is the sum of the outputs of the generator for limit = 5?**


```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

sum = 0
for sqrt_value in generator:
    sum += sqrt_value
    print(sqrt_value)

print(sum)
```

    1.0
    1.4142135623730951
    1.7320508075688772
    2.0
    2.23606797749979
    8.382332347441762



- **Question 2: What is the 13th number yielded**



```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

for sqrt_value in generator:
    print(sqrt_value)


```

    1.0
    1.4142135623730951
    1.7320508075688772
    2.0
    2.23606797749979
    2.449489742783178
    2.6457513110645907
    2.8284271247461903
    3.0
    3.1622776601683795
    3.3166247903554
    3.4641016151377544
    3.605551275463989


# 2. Append a generator to a table with existing data


Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data

1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
2. Append the second generator to the same table as the first.
3. **After correctly appending the data, calculate the sum of all ages of people.**





```python
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

```

    {'ID': 1, 'Name': 'Person_1', 'Age': 26, 'City': 'City_A'}
    {'ID': 2, 'Name': 'Person_2', 'Age': 27, 'City': 'City_A'}
    {'ID': 3, 'Name': 'Person_3', 'Age': 28, 'City': 'City_A'}
    {'ID': 4, 'Name': 'Person_4', 'Age': 29, 'City': 'City_A'}
    {'ID': 5, 'Name': 'Person_5', 'Age': 30, 'City': 'City_A'}
    {'ID': 3, 'Name': 'Person_3', 'Age': 33, 'City': 'City_B', 'Occupation': 'Job_3'}
    {'ID': 4, 'Name': 'Person_4', 'Age': 34, 'City': 'City_B', 'Occupation': 'Job_4'}
    {'ID': 5, 'Name': 'Person_5', 'Age': 35, 'City': 'City_B', 'Occupation': 'Job_5'}
    {'ID': 6, 'Name': 'Person_6', 'Age': 36, 'City': 'City_B', 'Occupation': 'Job_6'}
    {'ID': 7, 'Name': 'Person_7', 'Age': 37, 'City': 'City_B', 'Occupation': 'Job_7'}
    {'ID': 8, 'Name': 'Person_8', 'Age': 38, 'City': 'City_B', 'Occupation': 'Job_8'}



```python
import dlt

# define the connection to load to.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='homework')


# load any generator to a table at the pipeline destination:
info = generators_pipeline.run(people_1(),
    table_name="people",
    write_disposition="replace")

# print the outcome
print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.16 seconds
    1 load package(s) were loaded to destination duckdb and into dataset homework
    The duckdb destination used duckdb:////home/andre/dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708107577.5684946 is LOADED and contains no failed jobs



```python
import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# display the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))
```

    Loaded tables: 



    ┌─────────────────────┐
    │        name         │
    │       varchar       │
    ├─────────────────────┤
    │ _dlt_loads          │
    │ _dlt_pipeline_state │
    │ _dlt_version        │
    │ people              │
    │ people_merge        │
    └─────────────────────┘



```python
# display the data
print("people table:")

my_people = conn.sql("SELECT * FROM people").df()
display(my_people)
```

    people table:



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>age</th>
      <th>city</th>
      <th>_dlt_load_id</th>
      <th>_dlt_id</th>
      <th>occupation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Person_1</td>
      <td>26</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>TwOz2CKndXm96g</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Person_2</td>
      <td>27</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>cm4dQpvPGgUMAw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Person_3</td>
      <td>28</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>Qwqu2ZOfCLazjg</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Person_4</td>
      <td>29</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>04c2jHlBYAianw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Person_5</td>
      <td>30</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>v8D3s4Gm7lmrTA</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



```python
info = generators_pipeline.run(people_2(),
           table_name="people",
           write_disposition="append")

print("people table:")

my_people = conn.sql("SELECT * FROM people").df()
display(my_people)

display(conn.sql("SELECT SUM(age) FROM people"))
```

    people table:



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>age</th>
      <th>city</th>
      <th>_dlt_load_id</th>
      <th>_dlt_id</th>
      <th>occupation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Person_1</td>
      <td>26</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>TwOz2CKndXm96g</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Person_2</td>
      <td>27</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>cm4dQpvPGgUMAw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Person_3</td>
      <td>28</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>Qwqu2ZOfCLazjg</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Person_4</td>
      <td>29</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>04c2jHlBYAianw</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Person_5</td>
      <td>30</td>
      <td>City_A</td>
      <td>1708107577.5684946</td>
      <td>v8D3s4Gm7lmrTA</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>Person_3</td>
      <td>33</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>NETeM//JOne0tg</td>
      <td>Job_3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>Person_4</td>
      <td>34</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>/hubDLRSgqXhWQ</td>
      <td>Job_4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>Person_5</td>
      <td>35</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>U6L6SRe80qiLeQ</td>
      <td>Job_5</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>Person_6</td>
      <td>36</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>NkJ5P3IsU8tLTg</td>
      <td>Job_6</td>
    </tr>
    <tr>
      <th>9</th>
      <td>7</td>
      <td>Person_7</td>
      <td>37</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>wyFlQr0kCO14+w</td>
      <td>Job_7</td>
    </tr>
    <tr>
      <th>10</th>
      <td>8</td>
      <td>Person_8</td>
      <td>38</td>
      <td>City_B</td>
      <td>1708107972.0784695</td>
      <td>PckwkVynMJ5EwQ</td>
      <td>Job_8</td>
    </tr>
  </tbody>
</table>
</div>



    ┌──────────┐
    │ sum(age) │
    │  int128  │
    ├──────────┤
    │      353 │
    └──────────┘


# 3. Merge a generator

Re-use the generators from Exercise 2.

A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

After loading, you should have a total of 8 records, and ID 3 should have age 33.

Question: **Calculate the sum of ages of all the people loaded as described above.**



```python
# load people_1 generator to a new table 
info = generators_pipeline.run(people_1(),
           table_name="people_merge",
           write_disposition="replace")

# print the outcome
print(info)
```

    Pipeline dlt_ipykernel_launcher load step completed in 0.17 seconds
    1 load package(s) were loaded to destination duckdb and into dataset homework
    The duckdb destination used duckdb:////home/andre/dlt_ipykernel_launcher.duckdb location to store data
    Load package 1708109530.346911 is LOADED and contains no failed jobs



```python
# display the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# display the data
print("people table:")
display(conn.sql("SELECT * FROM people_merge"))
```

    Loaded tables: 



    ┌─────────────────────┐
    │        name         │
    │       varchar       │
    ├─────────────────────┤
    │ _dlt_loads          │
    │ _dlt_pipeline_state │
    │ _dlt_version        │
    │ people              │
    │ people_merge        │
    └─────────────────────┘


    people table:



    ┌───────┬──────────┬───────┬─────────┬───────────────────┬────────────────┬────────────┐
    │  id   │   name   │  age  │  city   │   _dlt_load_id    │    _dlt_id     │ occupation │
    │ int64 │ varchar  │ int64 │ varchar │      varchar      │    varchar     │  varchar   │
    ├───────┼──────────┼───────┼─────────┼───────────────────┼────────────────┼────────────┤
    │     1 │ Person_1 │    26 │ City_A  │ 1708109530.346911 │ aOjR0R429rhwYw │ NULL       │
    │     2 │ Person_2 │    27 │ City_A  │ 1708109530.346911 │ MsAHWzm2uj95fw │ NULL       │
    │     3 │ Person_3 │    28 │ City_A  │ 1708109530.346911 │ kIw00XlhTazg4Q │ NULL       │
    │     4 │ Person_4 │    29 │ City_A  │ 1708109530.346911 │ TtAGTUPPRQhYsQ │ NULL       │
    │     5 │ Person_5 │    30 │ City_A  │ 1708109530.346911 │ NJ2eF64lywIAVw │ NULL       │
    └───────┴──────────┴───────┴─────────┴───────────────────┴────────────────┴────────────┘



```python
# run the pipeline with default settings, and capture the outcome
info = generators_pipeline.run(people_2(),
          table_name="people_merge",
          write_disposition="merge",
          primary_key='id')

# display the data
print("merged table:")
display(conn.sql("SELECT * FROM people_merge"))

# calculate and display the sum
display(conn.sql("SELECT SUM(age) FROM people_merge"))
```

    merged table:



    ┌───────┬──────────┬───────┬─────────┬────────────────────┬────────────────┬────────────┐
    │  id   │   name   │  age  │  city   │    _dlt_load_id    │    _dlt_id     │ occupation │
    │ int64 │ varchar  │ int64 │ varchar │      varchar       │    varchar     │  varchar   │
    ├───────┼──────────┼───────┼─────────┼────────────────────┼────────────────┼────────────┤
    │     1 │ Person_1 │    26 │ City_A  │ 1708109530.346911  │ aOjR0R429rhwYw │ NULL       │
    │     2 │ Person_2 │    27 │ City_A  │ 1708109530.346911  │ MsAHWzm2uj95fw │ NULL       │
    │     8 │ Person_8 │    38 │ City_B  │ 1708109635.2221606 │ VM0cNQMNY8SwUg │ Job_8      │
    │     5 │ Person_5 │    35 │ City_B  │ 1708109635.2221606 │ qgnc3Rdmzobl7A │ Job_5      │
    │     7 │ Person_7 │    37 │ City_B  │ 1708109635.2221606 │ l8YGNcccMSQApQ │ Job_7      │
    │     4 │ Person_4 │    34 │ City_B  │ 1708109635.2221606 │ vEO+rVPRJ6sYgQ │ Job_4      │
    │     3 │ Person_3 │    33 │ City_B  │ 1708109635.2221606 │ Bmakgr0Vq3Y2Mw │ Job_3      │
    │     6 │ Person_6 │    36 │ City_B  │ 1708109635.2221606 │ m4Wb8K63M61rIg │ Job_6      │
    └───────┴──────────┴───────┴─────────┴────────────────────┴────────────────┴────────────┘



    ┌──────────┐
    │ sum(age) │
    │  int128  │
    ├──────────┤
    │      266 │
    └──────────┘






