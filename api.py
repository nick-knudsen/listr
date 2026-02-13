from datetime import date

import duckdb
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from hotspot_optimizer import optimize_hotspots

DB_PATH = "data/vermont.duckdb"

app = FastAPI(title="Listr")


class OptimizeRequest(BaseModel):
    life_list: list[str]
    start_date: date
    end_date: date
    k: int = 5
    county: str | None = None
    state: str | None = None


@app.get("/api/counties")
def get_counties():
    """Return the list of available counties."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        rows = con.execute(
            "SELECT DISTINCT county FROM sightings_clean ORDER BY county"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


@app.post("/api/optimize")
def run_optimization(req: OptimizeRequest):
    """Run the hotspot optimization.

    The client sends the user's life list (species names) along with
    the search parameters. No user data is stored on the server.
    """
    result = optimize_hotspots(
        db_path=DB_PATH,
        life_list_names=req.life_list,
        start_date=req.start_date,
        end_date=req.end_date,
        k=req.k,
        county=req.county,
        state=req.state,
    )

    return {
        "total_expected_lifers": round(result.total_expected_lifers, 2),
        "num_candidate_hotspots": result.num_candidate_hotspots,
        "num_potential_lifers": result.num_potential_lifers,
        "date_range": [result.date_range[0].isoformat(), result.date_range[1].isoformat()],
        "geographic_filter": result.geographic_filter,
        "hotspots": [
            {
                "rank": h.rank,
                "locality": h.locality,
                "locality_id": h.locality_id,
                "latitude": h.latitude,
                "longitude": h.longitude,
                "county": h.county,
                "marginal_gain": round(h.marginal_gain, 2),
                "cumulative_expected": round(h.cumulative_expected, 2),
                "target_species": [
                    {"common_name": sp.common_name, "probability": round(sp.probability, 4)}
                    for sp in h.target_species
                ],
            }
            for h in result.selected_hotspots
        ],
        "species_combined_probs": [
            {"common_name": sp.common_name, "probability": round(sp.probability, 4)}
            for sp in result.species_combined_probs
        ],
    }


# Serve the frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")
