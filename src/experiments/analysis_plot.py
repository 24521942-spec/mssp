# src/experiments/analysis_plot.py
import pandas as pd, seaborn as sns, matplotlib.pyplot as plt

def heatmap_postds(csv_path="experiment_results.csv"):
    df = pd.read_csv(csv_path)
    pv = df.pivot_table(index="proc", columns="net", values="post_sort_double_spends", aggfunc="mean")
    sns.heatmap(pv, annot=True, cmap="YlOrRd")
    plt.title("Post-Sort Double-Spend vs (ProcessMean, NetworkMean)")
    plt.ylabel("process mean (s)"); plt.xlabel("network mean (s)")
    plt.tight_layout(); plt.savefig("heatmap_postds.png", dpi=200); plt.show()

def pbft_by_malicious(csv_path="experiment_results.csv"):
    df = pd.read_csv(csv_path)
    g = df.groupby("mal")[["pbft_success","pbft_failure"]].mean().reset_index()
    g.plot(x="mal", y=["pbft_success","pbft_failure"], kind="bar")
    plt.title("PBFT Success/Failure vs Malicious%")
    plt.ylabel("avg count per run"); plt.xlabel("malicious fraction")
    plt.tight_layout(); plt.savefig("pbft_bar.png", dpi=200); plt.show()

if __name__ == "__main__":
    heatmap_postds(); pbft_by_malicious()
