import streamlit as st

# Configure page
st.set_page_config(
    page_title="Browser Test",
    page_icon="🔍",
    layout="wide"
)

st.title("Browser Diagnostics")

# Test JavaScript execution
st.header("JavaScript Test")
st.markdown("""
<script>
    document.write('<div id="js-test">JavaScript is working!</div>');
</script>
""", unsafe_allow_html=True)

# Display Streamlit version
st.header("Streamlit Information")
st.write(f"Streamlit Version: {st.__version__}")

# Test iframe loading (maps often use iframes)
st.header("IFrame Test")
st.markdown("""
<iframe src="https://www.openstreetmap.org/export/embed.html?bbox=-0.004017949104309083%2C51.47612752641776%2C0.00030577182769775396%2C51.478569861898606&layer=mapnik" 
        width="600" 
        height="400" 
        style="border: 1px solid black">
</iframe>
""", unsafe_allow_html=True)

# Troubleshooting instructions
st.header("Troubleshooting Steps")
st.markdown("""
### If you don't see the map above, try these steps:

1. **Check Browser:**
   - Are you using Chrome? (recommended)
   - Try opening in an incognito/private window

2. **Check Browser Settings:**
   - Open browser settings
   - Search for "JavaScript" and ensure it's enabled
   - Disable ad blockers for localhost
   - Allow third-party cookies

3. **Clear Browser Data:**
   - Open browser settings
   - Clear cache and cookies
   - Restart browser

4. **Check Network:**
   - Press Cmd+Option+I (Mac) or Ctrl+Shift+I (Windows)
   - Go to Network tab
   - Refresh page
   - Look for any red (blocked) requests

5. **Try Different URL:**
   - If using localhost, try the Network URL
   - If using Network URL, try localhost
""") 