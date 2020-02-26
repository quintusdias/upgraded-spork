import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

sns.set()

class PHLPlot(object):

    def __init__(self):
        self.colors = sns.xkcd_palette(['faded green'])
        self.conn = psycopg2.connect(dbname='phl')
        fig, self.ax = plt.subplots(nrows=2, ncols=3)

    def run(self):
        self.summarize_detection_method()

    def summarize_detection_method(self):

        sql = """
           select detection, count(*) as n
             from planets
         group by detection
         order by n
        """
        df = pd.read_sql(sql, self.conn, index_col='detection')
        
        ax = self.ax[0, 1]
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
        
            p.set_color(self.colors[0])
        
        plt.tight_layout()


if __name__ == '__main__':

    o = PHLPlot()
    o.run()
