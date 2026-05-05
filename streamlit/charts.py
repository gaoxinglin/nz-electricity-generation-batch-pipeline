import pandas as pd
import plotly.graph_objects as go

FUEL_COLORS: dict[str, str] = {
    "Hydro": "#2196F3",
    "Geothermal": "#FF5722",
    "Wind": "#4CAF50",
    "Gas": "#9E9E9E",
    "Coal": "#37474F",
    "Co-Gen": "#FF9800",
    "Solar": "#FFC107",
    "Wood": "#8D6E63",
    "Battery": "#AB47BC",
    "Diesel": "#F44336",
    "Gas_Thermal": "#78909C",
    "Gas Thermal": "#78909C",
}
_FALLBACK = "#607D8B"


def fuel_color(name: str) -> str:
    return FUEL_COLORS.get(name, _FALLBACK)


def color_map(fuel_types: list[str]) -> dict[str, str]:
    return {ft: fuel_color(ft) for ft in fuel_types}


def fmt_month(ym: str | int) -> str:
    """'202301' or 202301 → 'Jan 2023'."""
    return pd.to_datetime(str(ym).zfill(6), format="%Y%m").strftime("%b %Y")


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def apply_layout(fig: go.Figure, height: int = 480) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=16, r=16, t=40, b=16),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=13),
    )
    fig.update_xaxes(showgrid=False, tickangle=-30)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.12)")
    return fig


def sparkline(values: pd.Series, color: str = "#00897B") -> go.Figure:
    y = values.tolist()
    fig = go.Figure(go.Scatter(
        x=list(range(len(y))),
        y=y,
        mode="lines",
        line=dict(color=color, width=2),
        fill="tozeroy",
        fillcolor=_hex_to_rgba(color, 0.12),
    ))
    fig.update_layout(
        height=55,
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    return fig
