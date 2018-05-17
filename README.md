# Tools for GBDx
There are two sets of tools here, and a script for launching these tools.
1. GOST_GBDx_Tools/gbdxTasks.py: This library is used to launch GBDx tasks. See createTasks to launch GBDx tasks that are based on the output script from Charles' searching script
2. GOST_GBDx_Tools/gbdxURL_misc: Manipulates the GBDx url fetches - useful for monitoring workflows and listing/downloading results. **Warning, the AWSCLI stuff is off-the-wall weird because of WBG limitations of web-based command line tools - discuss with Ben**

See the launch_* tools for executing these tools.

# Setup
1. Install gbdxtools **in a new conda environment, this has caused problems for Clara**
2. Test the gbdx install. 
`python -c "import gbdxtools"`
3. 
