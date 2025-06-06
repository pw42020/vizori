import random

import pandas as pd
import plotly.express as px
import plotly.io as pio

random.seed(1)

LANDING_PAGE_OPTS: list[str] = [
    "Tutorial",
    "Product Info",
    "Daily Free Paywall",
    "Pricing",
    "Performance Paywall",
]
MALE_CONVERSION: list[float] = [0.1, 0.2, 0.1, 0.1, 0.5]
FEMALE_CONVERSION: list[float] = [0.05, 0.1, 0.5, 0.15, 0.2]

N_ROWS: int = 200


rows = []
for page_name, male_conversion, female_conversion in zip(
    LANDING_PAGE_OPTS, MALE_CONVERSION, FEMALE_CONVERSION
):

    for jj in range(N_ROWS):
        # Randomly choose a conversion success based on the

        male: bool = random.random() < 0.5
        conversion = male_conversion if male else female_conversion
        conversion_success = random.random() <= conversion
        rows.append(
            {
                "page_name": page_name,
                "landing_page_name": page_name,
                "age": int(random.random() * 100),
                "sex": "male" if male else "female",
                "conversion_success": conversion_success,
            }
        )

df = pd.DataFrame(rows)

# sum conversion success by landing page and sex
updated_df = (
    df.groupby(["landing_page_name", "sex"])
    .agg(
        conversion_success=("conversion_success", "sum"),
        total_count=("conversion_success", "count"),
    )
    .reset_index()
)

print(updated_df)

fig = px.histogram(
    updated_df,
    x="landing_page_name",
    y="conversion_success",
    color="sex",
)

# Convert to JSON
plot_json = pio.to_json(fig, pretty=True)

# Save to file or send via API
with open("plot_data.json", "w") as f:
    f.write(plot_json)

with open("business_graph.png", "wb") as f:
    f.write(fig.to_image(format="png", width=1600, height=900))
