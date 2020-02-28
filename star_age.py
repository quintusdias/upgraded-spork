import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

colors = sns.xkcd_palette(['pale yellow'])

conn = psycopg2.connect(dbname='phl')
cursor = conn.cursor()

# Summarize the planet detection methods.
sql = """
select
    case
        when age < 1 then '< 1'
        when age < 5 then floor(age)::text || '-' || floor(age+1)::text
        when age >= 5 then '> 5'
        when age is null then 'No Data'
    end star_age,
    count(*) as n
from stars
group by star_age
order by star_age
"""
df = pd.read_sql(sql, conn, index_col='star_age')

fig, ax = plt.subplots()
h = df.plot.bar(ax=ax, legend=None)
ax.set_title('Stellar Ages')
ax.set_ylabel('Number of Stars')
ax.set_xlabel('Age (Gy)')

for p in ax.patches:
    height = p.get_height()
    ax.annotate(f"{height}",
                xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                xytext=(3, 0), # 3 points horizontal offset
                textcoords="offset points",
                ha='center', va='bottom')

    p.set_color(colors[0])

ax.tick_params(axis='x', rotation=0)
