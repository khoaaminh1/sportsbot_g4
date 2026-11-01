import os
import streamlit as st
from sportsbot.core import answer

st.set_page_config(page_title='Vietnam SportsBot', layout='wide')
for key in ('LLM_API_KEY', 'LLM_BASE_URL', 'LLM_MODEL'):
    try:
        if key in st.secrets:
            os.environ[key] = str(st.secrets[key])
    except FileNotFoundError:
        pass
st.title('Vietnam SportsBot')
st.caption('Vietnamese sports news with original sources. RSS coverage is limited; this is not a live score service.')
with st.sidebar:
    refresh = st.checkbox('Refresh RSS when asking', value=True)
    st.caption('Successful feeds are cached for five minutes. Collection stops when the application is not running.')
    if st.button('Clear conversation'):
        st.session_state.messages = []
if 'messages' not in st.session_state:
    st.session_state.messages = []


def display(result):
    st.write(result['answer'])
    if result['warning']:
        st.warning(result['warning'])
    for source in result['sources']:
        st.link_button(f"[{source['id']}] {source['title']}", source['url'])
        st.caption(f"{source['source']} | Published: {source['published_at'] or 'unknown'} | Collected: {source['collected_at']}")
    with st.expander('Collection status'):
        st.json(result['collection'])


for message in st.session_state.messages:
    with st.chat_message(message['role']):
        if message['role'] == 'user':
            st.write(message['content'])
        else:
            display(message['result'])
if question := st.chat_input('Ask about a team, athlete, match or event'):
    history = [{'role': m['role'], 'content': m['content']} for m in st.session_state.messages[-6:]]
    with st.chat_message('user'):
        st.write(question)
    with st.chat_message('assistant'):
        with st.spinner('Checking sources...'):
            result = answer(question, history, refresh)
        display(result)
    st.session_state.messages.extend([{'role': 'user', 'content': question}, {'role': 'assistant', 'content': result['answer'], 'result': result}])
