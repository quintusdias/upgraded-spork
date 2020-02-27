import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

colors = sns.xkcd_palette(['lavender'])

conn = psycopg2.connect(dbname='phl')
cursor = conn.cursor()

# Summarize the planet detection methods.
sql = """
   select year_discovered, count(*) as n
     from planets
 group by year_discovered
 order by year_discovered
"""
df = pd.read_sql(sql, conn, index_col='year_discovered')

fig, ax = plt.subplots()
h = df.plot.bar(ax=ax, legend=None)
ax.set_title('Planet Discovery Years')
ax.set_ylabel('Number of Exoplanets')
ax.set_xlabel(None)

for p in ax.patches:
    height = p.get_height()
    ax.annotate(f"{height}",
                xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                xytext=(3, 0), # 3 points horizontal offset
                textcoords="offset points",
                ha='center', va='bottom',
                fontsize=6)

    p.set_color(colors[0])

# reset the xtick labels
h = ax.get_xticklabels()

newlabels = [
    h.get_text() if int(h.get_text()) % 5 == 0 else ''
    for h in ax.get_xticklabels()
]
ax.set_xticklabels(newlabels)
#ax.set_xlim(1987, 2021)
ax.tick_params(axis='x', rotation=0)
#box = ax.get_position()
#ax.set_position([0.41, 0.139, 0.45, 0.777])

