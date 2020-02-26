import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

colors = sns.xkcd_palette(['apricot'])

conn = psycopg2.connect(dbname='phl')
cursor = conn.cursor()

# Summarize the planet detection methods.
sql = """
   select type, count(*) as n
     from planets
 group by type
 order by n
"""
df = pd.read_sql(sql, conn, index_col='type')
df.index = [
    'Mini\nTerran',
    'Unknown',
    'Sub\nTerran',
    'Terran', 
    'Neptunian',
    'Super\nTerran',
    'Jovian'
]

fig, ax = plt.subplots()
h = df.plot.bar(ax=ax, legend=None)
ax.set_title('Planet Types')
ax.set_ylabel('Number of Exoplanets')
ax.set_xlabel(None)

for p in ax.patches:
    height = p.get_height()
    ax.annotate(f"{height}",
                xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                xytext=(3, 0), # 3 points horizontal offset
                textcoords="offset points",
                ha='center', va='bottom')

    p.set_color(colors[0])

ax.set_ylim(0, 1400)
ax.tick_params(axis='x', rotation=0)
#box = ax.get_position()
#ax.set_position([0.41, 0.139, 0.45, 0.777])

