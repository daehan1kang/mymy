import altair as alt
import panel as pn
import param
import polars as pl


class ColumnPlot(pn.viewable.Viewer):
    df: pl.DataFrame = param.ClassSelector(class_=pl.DataFrame, allow_refs=True)
    url = param.String(default=None, allow_refs=True, allow_None=True)

    column: str = param.String(allow_refs=True)
    column_groups = param.Dict(allow_refs=True)
    describe_fn = param.Callable()

    chart = param.ClassSelector(class_=(alt.Chart, dict))

    def __init__(self, **params):
        super().__init__(**params)
        # 1. Create the widget once during initialization
        self._column_select = pn.widgets.Select(
            name="Select Column",
            groups=self.column_groups,
            value=self.column,
            sizing_mode="stretch_width",
        )
        # 2. Establish bidirectional links
        self._column_select.link(
            self, value="column", groups="column_groups", bidirectional=True
        )

    @param.depends("df", watch=True, on_init=True)
    def update_source(self):
        # 1. Categorize columns for UI groups
        column_groups = {
            "🔢 Numerical": [],
            "🔤 String / Boolean": [],
            "📅 Temporal": [],
            "📦 Others": [],
        }

        for col in self.df.columns:
            dtype = self.df.schema[col]
            item = col

            if dtype.is_numeric():
                column_groups["🔢 Numerical"].append(item)
            elif dtype in [pl.String, pl.Boolean]:
                column_groups["🔤 String / Boolean"].append(item)
            elif dtype.is_temporal():
                column_groups["📅 Temporal"].append(item)
            else:
                column_groups["📦 Others"].append(item)

        # 2. Filter empty groups
        self.column_groups = {k: v for k, v in column_groups.items() if v}

        # 3. Set initial value if empty
        if not self.column or self.column not in self.df.columns:
            self.column = self.df.columns[0]

        self.param.trigger("column")

    def draw_chart(self):
        source = self.url if self.url else self.df

        if isinstance(source, str):
            source = alt.Data(url=source, format=alt.CsvDataFormat(type="csv"))

        dtype = self.df.schema[self.column]

        base = alt.Chart(source).properties(width="container", height=300)

        if dtype.is_float():
            res = base.mark_bar().encode(
                x=alt.X(f"{self.column}:Q", bin=alt.Bin(maxbins=30), title=self.column),
                y=alt.Y("count()", title="Frequency"),
                tooltip=[alt.Tooltip(f"{self.column}:Q", bin=True), "count()"],
            )
        elif dtype == pl.String or dtype == pl.Boolean or dtype.is_integer():
            res = base.mark_bar().encode(
                x=alt.X(f"{self.column}:N", sort="-y", title=self.column),
                y=alt.Y("count()", title="Frequency"),
                tooltip=[f"{self.column}:N", "count()"],
            )
        elif dtype.is_temporal():
            res = base.mark_bar().encode(
                x=alt.X(f"{self.column}:T", bin=True, title=self.column),
                y=alt.Y("count()", title="Frequency"),
                tooltip=[alt.Tooltip(f"{self.column}:T", bin=True), "count()"],
            )
        else:
            res = base.mark_text().encode(text=alt.value(f"Unsupported: {dtype}"))

        return res

    @param.depends("df", "column", watch=True)
    def update_chart(self):
        self.chart = self.draw_chart()

    @param.depends("df", "column")
    def describe(self):
        s = self.df[self.column]

        if self.describe_fn is not None:
            return self.describe_fn(s)

        # Basic Metadata
        info = {
            "Name": self.column,
            "Type": str(s.dtype),
            "Rows": len(s),
            "Nulls": s.null_count(),
            "Unique": s.n_unique(),
        }

        # Numeric Specifics
        if s.dtype.is_numeric():
            info.update(
                {
                    "Mean": f"{s.mean():.2f}" if s.mean() is not None else "N/A",
                    "Std": f"{s.std():.2f}" if s.std() is not None else "N/A",
                    "Min": s.min(),
                    "Max": s.max(),
                }
            )

        return info

    def _render_stats(self, _):
        # Convert stats dict to HTML table (input '_' is required for pn.bind)
        data = self.describe()
        rows = [
            f"<tr><td style='padding:4px; border-bottom:1px solid #eee;'><b>{k}</b></td>"
            f"<td style='text-align:right; padding:4px; border-bottom:1px solid #eee;'>{v}</td></tr>"
            for k, v in data.items()
        ]
        return (
            f"<div style='border:1px solid #ddd; border-radius:4px; padding:8px;'>"
            f"<table style='width:100%; font-size:12px; border-collapse: collapse;'>{''.join(rows)}</table></div>"
        )

    def view(self):

        # 2. Controls column (fixed width, contains selector and stats)
        controls = pn.Column(
            self._column_select,
            pn.pane.HTML(
                pn.bind(self._render_stats, self.param.column),
                sizing_mode="stretch_width",
            ),
            width=280,
            margin=(0, 20, 0, 0),
        )

        # 3. Chart area (stretches to fill the rest of the row)
        chart_pane = pn.pane.Vega(self.param.chart, sizing_mode="stretch_width")

        return pn.Row(controls, chart_pane, sizing_mode="stretch_width")

    def __panel__(self):
        return self.view()


class DataExplorer(pn.viewable.Viewer):
    df: pl.DataFrame = param.ClassSelector(class_=pl.DataFrame, allow_refs=True)
    url: str = param.String(default=None, allow_refs=True)

    # 1. Search and Filter Parameters
    search_term = param.String(default="", label="Search Columns")
    type_filter = param.ListSelector(
        default=[],
        objects=["Numerical", "String / Boolean", "Temporal", "Others"],
        label="Filter by Type",
    )

    def __init__(self, **params):
        super().__init__(**params)
        self._all_plots = []
        self._update_all_plots()

    @param.depends("df", watch=True)
    def _update_all_plots(self):
        # Create all plots once when df changes
        if self.df is not None:
            self._all_plots = [
                ColumnPlot(df=self.df, url=self.url, column=col)
                for col in self.df.columns
            ]

    @param.depends("search_term", "type_filter")
    def _get_filtered_plots(self):
        # Logic to filter plots based on search and types
        filtered = self._all_plots

        # Filter by search term
        if self.search_term:
            filtered = [
                p for p in filtered if self.search_term.lower() in p.column.lower()
            ]

        # Filter by type (using the internal groups logic of ColumnPlot)
        if self.type_filter:
            selected_plots = []
            for p in filtered:
                dtype = self.df.schema[p.column]
                category = "Others"
                if dtype.is_numeric():
                    category = "Numerical"
                elif dtype in [pl.String, pl.Boolean]:
                    category = "String / Boolean"
                elif dtype.is_temporal():
                    category = "Temporal"

                if category in self.type_filter:
                    selected_plots.append(p)
            filtered = selected_plots

        return pn.FlexBox(
            *filtered, justify_content="start", sizing_mode="stretch_width"
        )

    def view(self):
        # 2. Build Sidebar-like controls and Main area
        search_box = pn.widgets.TextInput.from_param(
            self.param.search_term, placeholder="Filter by name..."
        )
        type_check = pn.widgets.CheckButtonGroup.from_param(self.param.type_filter)

        toolbar = pn.Row(
            pn.Column("### 🔍 Search", search_box, width=250),
            pn.Column("### 🛠 Type Filter", type_check),
            sizing_mode="stretch_width",
            margin=(10, 10, 20, 10),
            styles={"background": "#f9f9f9", "border-radius": "5px", "padding": "10px"},
        )

        return pn.Column(
            pn.pane.Markdown(f"# Data Explorer"),
            toolbar,
            self._get_filtered_plots,  # Reactive filtering area
            sizing_mode="stretch_width",
        )

    def __panel__(self):
        return self.view()
