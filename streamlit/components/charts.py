import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.theme import PLOTLY_LAYOUT, REGION_COLOURS, MOMENTUM_COLOURS


def _apply(fig, height=380):
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    return fig


def bar_top_videos(df: pd.DataFrame, region: str) -> go.Figure:
    df = df.sort_values("view_count")
    fig = go.Figure(go.Bar(
        x=df["view_count"],
        y=df["title"].str[:45] + "…",
        orientation="h",
        marker=dict(
            color=df["engagement_rate"],
            colorscale=[[0, "#1a2236"], [0.5, "#3b82f6"], [1, "#ff4b4b"]],
            showscale=True,
            colorbar=dict(
                title=dict(text="Engagement", font=dict(color="#64748b", size=10)),
                tickfont=dict(color="#64748b", size=9),
                thickness=10,
            ),
        ),
        hovertemplate="<b>%{y}</b><br>Views: %{x:,}<extra></extra>",
    ))
    fig.update_layout(title=f"Top 10 — {region}", xaxis_title="Views")
    return _apply(fig, 400)


def scatter_engagement(df: pd.DataFrame) -> go.Figure:
    fig = px.scatter(
        df,
        x="view_count",
        y="engagement_rate",
        size="like_count",
        color="category_name",
        hover_name="title",
        hover_data={"channel_title": True, "trending_rank": True,
                    "view_count": False, "engagement_rate": False, "like_count": False},
        color_discrete_sequence=["#ff4b4b","#3b82f6","#f59e0b","#22c55e",
                                  "#a855f7","#06b6d4","#ec4899","#84cc16"],
        labels={"view_count": "Views", "engagement_rate": "Engagement Rate",
                "category_name": "Category"},
        title="Views vs Engagement",
    )
    fig.update_traces(marker=dict(opacity=0.85, line=dict(width=0)))
    return _apply(fig, 400)


def bar_top_channels(df: pd.DataFrame) -> go.Figure:
    top = df.nlargest(12, "total_views").sort_values("total_views")
    colours = [REGION_COLOURS.get(r, "#64748b") for r in top["region"]]
    fig = go.Figure(go.Bar(
        x=top["total_views"],
        y=top["channel_title"].str[:35],
        orientation="h",
        marker_color=colours,
        hovertemplate="<b>%{y}</b><br>Views: %{x:,}<extra></extra>",
    ))
    fig.update_layout(title="Top Channels by Views", xaxis_title="Total Views")
    return _apply(fig, 440)


def scatter_channel_dominance(df: pd.DataFrame) -> go.Figure:
    fig = px.scatter(
        df,
        x="trending_video_count",
        y="total_views",
        size="avg_engagement_rate_pct",
        color="region",
        color_discrete_map=REGION_COLOURS,
        hover_name="channel_title",
        hover_data={"channel_rank": True, "view_share_pct": True,
                    "trending_video_count": False, "total_views": False,
                    "avg_engagement_rate_pct": False},
        title="Channel Dominance",
        labels={"trending_video_count": "Trending Videos",
                "total_views": "Total Views", "region": "Region"},
    )
    fig.update_traces(marker=dict(opacity=0.85, line=dict(width=0)))
    return _apply(fig, 400)


def bar_category_views(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(
        df,
        x="category_name",
        y="total_views",
        color="region",
        barmode="group",
        color_discrete_map=REGION_COLOURS,
        title="Views by Category & Region",
        labels={"category_name": "", "total_views": "Total Views", "region": "Region"},
    )
    fig.update_layout(xaxis_tickangle=-35)
    return _apply(fig, 400)


def bar_category_engagement(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("category_name")["avg_engagement_rate_pct"].mean().reset_index()
    agg = agg.sort_values("avg_engagement_rate_pct")
    fig = go.Figure(go.Bar(
        x=agg["avg_engagement_rate_pct"],
        y=agg["category_name"],
        orientation="h",
        marker=dict(
            color=agg["avg_engagement_rate_pct"],
            colorscale=[[0, "#1a2236"], [1, "#ff4b4b"]],
            showscale=False,
        ),
        hovertemplate="%{y}: %{x:.3f}%<extra></extra>",
    ))
    fig.update_layout(title="Avg Engagement by Category", xaxis_title="Engagement %")
    return _apply(fig, 400)


def treemap_category(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("category_name")["total_views"].sum().reset_index()
    fig = px.treemap(
        agg, path=["category_name"], values="total_views",
        title="Category View Share",
        color="total_views",
        color_continuous_scale=[[0, "#111827"], [0.5, "#1e3a5f"], [1, "#3b82f6"]],
    )
    fig.update_traces(
        textfont=dict(family="DM Sans", color="white"),
        marker=dict(line=dict(width=2, color="#0a0e1a")),
    )
    fig.update_layout(coloraxis_showscale=False)
    return _apply(fig, 420)


def bar_momentum(df: pd.DataFrame) -> go.Figure:
    top = df.nlargest(15, "velocity_pct").sort_values("velocity_pct")
    colours = [MOMENTUM_COLOURS.get(m, "#64748b") for m in top["momentum_label"]]
    fig = go.Figure(go.Bar(
        x=top["velocity_pct"],
        y=top["title"].str[:40] + "…",
        orientation="h",
        marker_color=colours,
        text=top["velocity_pct"].apply(lambda v: f"+{v:.1f}%"),
        textposition="outside",
        textfont=dict(color="#94a3b8", size=10),
        hovertemplate="<b>%{y}</b><br>Velocity: +%{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(title="Fastest Rising Videos", xaxis_title="Day-over-Day Growth %")
    return _apply(fig, 480)


def donut_momentum(df: pd.DataFrame) -> go.Figure:
    counts = df["momentum_label"].value_counts()
    colours = [MOMENTUM_COLOURS.get(m, "#64748b") for m in counts.index]
    fig = go.Figure(go.Pie(
        labels=counts.index,
        values=counts.values,
        hole=0.6,
        marker=dict(colors=colours, line=dict(width=2, color="#0a0e1a")),
        textfont=dict(color="white", size=11),
        hovertemplate="%{label}: %{value} videos<extra></extra>",
    ))
    fig.update_layout(
        title="Momentum Mix",
        legend=dict(orientation="v", x=1, y=0.5),
        annotations=[dict(
            text=f"<b>{len(df)}</b><br><span style='font-size:10px'>videos</span>",
            x=0.5, y=0.5, font=dict(size=16, color="#e8edf5"),
            showarrow=False,
        )],
    )
    return _apply(fig, 340)
