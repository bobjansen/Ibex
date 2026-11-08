# SPDX-License-Identifier: AGPL-3.0-only
import json
from pathlib import Path
import tempfile
import unittest

from website_history import publish


def page(date, value=1):
    payload = {'meta': {'generated': date, 'commit': 'abc123'}, 'value': value}
    return ('<html><head><link href="./styles.css"></head><body>'
            '<div class="headline"></div><a href="./methodology.html">Code</a>'
            '<script>const PAYLOAD = ' + json.dumps(payload) + ';</script></body></html>')


class WebsiteHistory(unittest.TestCase):
    def test_publication_preserves_outgoing_pages_and_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / 'benchmarks.html'
            styles = root / 'styles.css'
            styles.write_text('original css')
            first = page('2026-10-24 12:00 UTC')
            code = '<html><head></head><a href="./benchmarks.html">Results</a>old code</html>'
            out.write_text(first)
            (root / 'methodology.html').write_text(code)
            publish(out, page('2026-10-25 12:00 UTC'), 'new code')
            history = json.loads((root / 'benchmark-history/index.json').read_text())
            self.assertEqual(len(history['runs']), 2)
            script = (root / 'benchmark-history/index.js').read_text()
            self.assertEqual(json.loads(script.split(' = ', 1)[1].rstrip(';\n')), history)
            old = history['runs'][1]
            archived = root / old['url']
            content = archived.read_bytes()
            self.assertIn(b'<base href="../../"', content)
            self.assertIn(f"benchmark-history/{old['id']}/methodology.html".encode(), content)
            frozen_code = archived.with_name('methodology.html').read_text()
            self.assertIn('old code', frozen_code)
            self.assertIn(old['url'], frozen_code)
            self.assertEqual(archived.with_name('styles.css').read_text(), 'original css')
            # Repeating a publication must not manufacture another entry.
            publish(out, page('2026-10-25 12:00 UTC'), 'new code')
            self.assertEqual(len(json.loads((root / 'benchmark-history/index.json').read_text())['runs']), 2)
            styles.write_text('changed css')
            # Same timestamp, different results still gets a separate permalink.
            publish(out, page('2026-10-25 12:00 UTC', 2), 'third code')
            self.assertEqual(archived.read_bytes(), content)
            self.assertEqual(archived.with_name('styles.css').read_text(), 'original css')
            self.assertEqual(out.read_text().count('id="benchmark-history"'), 1)

    def test_history_labels_each_suite_with_its_own_commit(self):
        # The suites are measured at different commits; the in-memory one
        # alone used to label the whole publication.
        suites = {'PAYLOAD': '950fafc1aaaa', 'H2O': 'b23a1390bbbb', 'PDSH': 'b23a1390bbbb'}
        body = ''.join(f'<script>const {name} = ' + json.dumps({'meta': {'commit': commit,
                       'generated': '2026-11-08 15:17 UTC'}}) + ';</script>' for name, commit in suites.items())
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / 'benchmarks.html'
            (Path(tmp) / 'styles.css').write_text('css')
            publish(out, '<html><head></head><body><div class="headline"></div>' + body + '</body></html>', 'code')
            run = json.loads((Path(tmp) / 'benchmark-history/index.json').read_text())['runs'][0]
            self.assertEqual(run['commit'], 'h2o + PDS-H b23a1390, in-memory 950fafc1')


if __name__ == '__main__':
    unittest.main()
