import argparse
import json
import time
from .core import answer, collect, connect


def show(result):
    print('\n' + result['answer'])
    if result['warning']:
        print('\nNotice:', result['warning'])
    for source in result['sources']:
        print(f"[{source['id']}] {source['source']} | published: {source['published_at'] or 'unknown'} | collected: {source['collected_at']}\n{source['url']}")
    print('Collection:', json.dumps(result['collection'], ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['ask', 'chat', 'ingest', 'export'])
    parser.add_argument('question', nargs='?', default='')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    if args.command == 'export':
        with connect() as db:
            for row in db.execute('SELECT * FROM articles'):
                print(json.dumps(dict(row), ensure_ascii=False))
    elif args.command == 'ingest':
        while True:
            print(json.dumps(collect(), ensure_ascii=False, indent=2))
            if not args.watch:
                break
            time.sleep(300)
    elif args.command == 'ask':
        if not args.question.strip():
            parser.error('Provide a question in quotes.')
        show(answer(args.question, refresh=not args.offline))
    else:
        history = []
        print('SportsBot. Type /exit to quit.')
        while True:
            try:
                question = input('You: ').strip()
            except (EOFError, KeyboardInterrupt):
                break
            if question == '/exit':
                break
            if not question:
                continue
            result = answer(question, history, not args.offline)
            show(result)
            history.extend([{'role': 'user', 'content': question}, {'role': 'assistant', 'content': result['answer']}])
            history = history[-6:]


if __name__ == '__main__':
    main()
