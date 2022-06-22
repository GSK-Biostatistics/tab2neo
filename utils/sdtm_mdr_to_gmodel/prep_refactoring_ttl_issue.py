from data_providers.data_provider import DataProvider

dp = DataProvider(debug = True,
                  mode = 'noschema')

classes = [
    "Start Date/Time of Observation",
    "Study",
    "Study Identifier",
    "Date/Time of First Study Treatment",
    "Age",
    "Race",
    "Date/Time of Birth",
    "Country",
    "Investigator Name",
    "Date/Time of Last Study Treatment",
    "Subject Death Flag",
    # "Study Day of Visit/Collection/Exam",
    "Investigator Identifier",
    "Unique Subject Identifier",
    "Subject",
    "Actual Arm Code",
    "Date/Time of Death",
    "Ethnicity",
    "Sex",
    "Study Site Identifier",
    "Description of Actual Arm",
    # "Subject Identifier for the Study",
    "Description of Planned Arm",
    "Date/Time of Informed Consent",
    "Age Units",
    "Domain",
    "Domain Abbreviation",
    "End Date/Time of Observation",
    # "Date/Time of Collection",
    "Planned Arm Code",
    "Date/Time of End of Participation"
]
data = dp.get_data(classes=classes, limit=None)
print(data)
print(data.columns)
#q = dp.qb.generate_query_body(classes)
#print(q)

p_classes = ["Study Day of Visit/Collection/Exam", "Subject Identifier for the Study", "Date/Time of Collection"]

