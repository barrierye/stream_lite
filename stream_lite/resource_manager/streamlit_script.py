#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-08-30
import streamlit as st
import pandas as pd
import numpy as np
import sys

from stream_lite.client.resource_manager_client import ResourceManagerClient

def main(client):
    st.title("StreamLite")

    if st.button('Update'):
        st.markdown("## Task Manager Location Table")
        map_df, latency_df = client.getAllTaskManagerDesc()
        st.table(map_df)
        st.markdown("## Task Manager Map")
        # https://wiki.openstreetmap.org/wiki/Zoom_levels
        st.map(map_df, zoom=15)
        
        st.markdown("## Task Manager Latency Table")
        st.table(latency_df)
    else:
        pass


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise Exception("Usage: streamlit run streamlit_script.py <endpoint>")
    resource_manager_endpoint = sys.argv[1]
    client = ResourceManagerClient()
    client.connect(resource_manager_endpoint)

    main(client)
