# Star-Schema
Data lake solution for bikeshare

Peace and Blessings. 
This project utilises raw data from a bike sharing program, converts and manipulates the data into a star schema format with 2 fact tables and 5 dimension tables. The star schema format is optimal for executing queries for business analysis at the cost of some data redundancy.
This project is designed to be imported into databricks and executed via a workflow.

<details>
<summary> Notebooks Snippet </summary>
![Notebook Image](./img/DBFS Snip)
</details>

# Design

<details>
<summary> Visual Database Designs </summary>
Conceptual Database Design
![Conceptual Database Design](https://user-images.githubusercontent.com/71145307/236345472-c392377c-1bca-4d33-943a-1028b44693d6.png)
Logical Database Design
![Logical Database Design](https://user-images.githubusercontent.com/71145307/236345484-09b63746-b989-47a9-ac9c-07893c3941ee.png)
Physical Database Design
![Physical Database Design](https://user-images.githubusercontent.com/71145307/236345494-c1ba720e-cf63-456c-ba21-b50b5d0c4bd7.png)
</details>

# Implementation

There are six notebooks in the databricks workflow and four csvs in this repository
SchemaDestruction - Functions as a reset by destroying the folder any old data/schemas/tables may be located in.
SchemaConstruction - Builds the schemas and dataframes that the raw data will be transfered to in and writes the directories and blank tables that store the processed data.
Bronze, Silver, Gold - Combine to implement the Medallion Architecture through ingesting, cleansing, manipulating and transforming the data into the desired format.
Queries - Has example business questions that a user may request and additionally has testing methods to assert that the data has been processed correctly.

Example Queries:

1) Total time spent riding based on specific dates
2) Total time spent riding by the hour
3) Total time spent riding by the day of the week
4) Total time spent riding by departure station
5) Total time spent riding by arrival station
6) Total time spent riding by departure and arrival station
7) Total time spent riding based on age of rider
8) Total time spent riding based on age of riders who are members
9) Total money spent per month
10) Total money spent per year
11) Total money spent per quarter
12) Total money spent by members based on age of rider when they became a member

# Evidence 

Please see below images of various aspects of the project:
<details>
<summary> DBFS Directory Layout </summary>
![image](https://user-images.githubusercontent.com/71145307/236349097-1c795175-a2ca-4b06-b918-db9bb6955fc9.png)
</details>

<details>
<summary> Fact Table </summary>
![image](https://user-images.githubusercontent.com/71145307/236349582-ca8f1f03-f68c-4019-8514-5d47687f155b.png)
</details>

<details>
<summary> Dimension Table </summary>
![image](https://user-images.githubusercontent.com/71145307/236349648-3c2bce52-f4fc-442c-aba1-3affc17513e3.png)
</details>

<details>
<summary> Example Query </summary>
![image](https://user-images.githubusercontent.com/71145307/236349258-11e50b00-6557-499e-91ae-35d66c92bad1.png)
</details>

<details>
<summary> Workflow </summary>
![image](https://user-images.githubusercontent.com/71145307/236353156-67686a0d-c144-48fe-996a-2c14e6497fc9.png)

</details>
