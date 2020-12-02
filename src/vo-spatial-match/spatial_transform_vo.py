import pandas as pd 
import geopandas as gpd 

from prefect import Flow
from prefect import task

@task
def _read_vo(path: str) -> gpd.GeoDataFrame:

    return gpd.read_parquet(path)

@task
def _read_geometries(path:str) -> gpd.GeoDataFrame:

    return gpd.read_parquet(path)

@task 
def _read_eds(path:str) -> gpd.GeoDataFrame:

    gdf = gpd.read_file(path)

    return gdf.to_crs(epsg="4326")


@task
def _merge_vo_sa(df: pd.DataFrame, gdf: gpd.GeoDataFrame, ) -> gpd.GeoDataFrame:

    return gpd.sjoin(df, gdf, how="inner")

@task
def _remove_index(df: pd.DataFrame) -> pd.DataFrame:

    return df.drop(['index_right'], axis=1)

@task
def _merge_vo_post(df: pd.DataFrame, gdf: gpd.GeoDataFrame, ) -> gpd.GeoDataFrame:

    return gpd.sjoin(df, gdf, how="inner")

@task
def _merge_vo_ed(df: pd.DataFrame, gdf: gpd.GeoDataFrame, ) -> gpd.GeoDataFrame:

    return gpd.sjoin(df, gdf, how="inner")


with Flow ("Assign SA, ED and Postcode to each VO building") as flow:

    vo = _read_vo("data/vo.parquet")
    sa = _read_geometries("data/sa_geometries.parquet")
    postcode = _read_geometries("data/dublin_postcodes.parquet")
    ed = _read_eds("data/ed/electoral_divisions.shp")
    vo_sa = _merge_vo_sa(vo, sa)
    vo_sa_dropped = _remove_index(vo_sa)
    vo_sa_post = _merge_vo_post(vo_sa_dropped, postcode)
    sa_post_dropped = _remove_index(vo_sa_post)
    vo_final = _merge_vo_ed(sa_post_dropped, ed)



if __name__ == "__main__":
    state = flow.run()