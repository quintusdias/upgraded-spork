import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

colors = sns.xkcd_palette(['faded green'])

conn = psycopg2.connect(dbname='phl')
cursor = conn.cursor()

# Summarize the planet detection methods.
sql = """
   select detection, count(*) as n
     from planets
 group by detection
 order by n
"""
df = pd.read_sql(sql, conn, index_col='detection')

fig, ax = plt.subplots()
h = df.plot.barh(ax=ax, legend=None)
ax.set_title('Planet Detection Methods')
ax.set_xlabel('Number of Exoplanets')
ax.set_ylabel(None)

for p in ax.patches:
    width = p.get_width()
    ax.annotate(f"{width}",
                xy=(width, p.get_y() + p.get_height() / 2),
                xytext=(3, 0), # 3 points horizontal offset
                textcoords="offset points",
                ha='left', va='center')

    p.set_color(colors[0])

ax.set_xlim(0, 4000)
box = ax.get_position()
ax.set_position([0.41, 0.139, 0.45, 0.777])

