import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

colors = sns.xkcd_palette(['light violet'])

conn = psycopg2.connect(dbname='phl')
cursor = conn.cursor()

# Summarize the planet detection methods.
sql = """
select n, count(*) from (
   select count(*) as n
     from planets
   group by star_id
) ct
group by n 
order by n
"""
df = pd.read_sql(sql, conn, index_col='n')

fig, ax = plt.subplots()
h = df.plot.bar(ax=ax, legend=None)
ax.set_title('Stellar Systems')
ax.set_ylabel('Number of Stars')
ax.set_xlabel('Planets Per Star')

for p in ax.patches:
    height = p.get_height()
    ax.annotate(f"{height}",
                xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                xytext=(3, 0), # 3 points horizontal offset
                textcoords="offset points",
                ha='center', va='bottom')

    p.set_color(colors[0])

ax.tick_params(axis='x', rotation=0)
#box = ax.get_position()
#ax.set_position([0.41, 0.139, 0.45, 0.777])

