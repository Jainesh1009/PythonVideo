import os
import vlc
from vlc import Instance
import time
import os
import webbrowser

import plotly.graph_objects as go

# fig = go.Figure(go.Scattergeo())
# fig.update_geos(projection_type="natural earth")
# fig.update_geos(fitbounds="locations")
# fig.update_layout(height=1020, margin={"r":0,"t":0,"l":0,"b":0})
# fig.show()
media_player = vlc.MediaPlayer("video1.mp4")
media_player.play()
time.sleep(5)
media_player.set_pause(1)
new = 2
# open a public URL, in this case, the webbrowser docs
url = 'http://localhost:3000/map/23.0438564/72.5086395/test'
webbrowser.register('chrome',
	None,
	webbrowser.BackgroundBrowser("C://Program Files//Google//Chrome//Application//chrome.exe"))
webbrowser.get('chrome').open_new(url)
time.sleep(15)
os.system("taskkill /im chrome.exe /f")
media_player.set_pause(0)
time.sleep(10)

