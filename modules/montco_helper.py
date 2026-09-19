import re
import feedparser
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

class MontCoDispatch():
    def __init__(self) -> None:
        self._entries = []
        self._listeners = set()  # set of tuples (municipality, timestamp)
    
    async def refresh(self) -> None:
        await self._refresh()

    async def _refresh(self) -> None:
        posted_entry_keys = self._get_posted_entry_keys()
        feed = await asyncio.to_thread(feedparser.parse,
            "https://webapp07.montcopa.org/eoc/cadinfo/livecadrss.asp")
        self._feed = feed.entries
        self._store_entries(posted_entry_keys)

    def _store_entries(self, posted_entry_keys=[]) -> None:
        self._entries = [self._parse_montco_entry(e.title,e.description)
            for e in self._feed]
        for e in self._entries:
            if self._entry_key(e) in posted_entry_keys:
                e['posted'] = datetime.now().timestamp()

    def get_messages(self):
        # return a list of formatted messages for the active entries
        active_entries = self._return_active()
        return [self._format_entry(e) for e in active_entries]

    def _format_entry(self, entry) -> str:
        # station may not always exist, so we need to handle that case
        if not entry['station']:
            return f"{entry['service']}: {entry['type']} at {entry['intersection']}, {entry['municipality']} at {datetime.fromtimestamp(entry['time']).strftime('%H:%M')}" 
        return f"{entry['service']}: {entry['type']} at {entry['intersection']}, {entry['municipality']} (Station: {entry['station']}) at {datetime.fromtimestamp(entry['time']).strftime('%H:%M')}"

    def add_listener(self, municipality, seconds):
        # add municipaility and the timestamp of now plus seconds to the listeners set
        self._listeners.add((municipality, datetime.now().timestamp() + seconds))
        # remove any listeners that have expired
        self._listeners = {(m, t) for (m, t) in self._listeners if t > datetime.now().timestamp()}

    def _entry_key(self, entry):
        return tuple(entry[k] for k in
            ("service","type","intersection","municipality","station","time"))

    def _get_posted_entry_keys(self):
        # return a list of entries that have already been posted
        return [self._entry_key(e) for e in self._entries if e.get('posted')]

    def _return_active(self):
        # return a list of entries for municipalities that are currently being listened for
        # and mark each entry as posted with the current timestamp
        # do not return any entries that have already been posted
        active_municipalities = self._return_active_municipalities()
        active_entries = [e for e in self._entries if e['municipality'] in active_municipalities and not e.get('posted')]
        for e in active_entries:
            e['posted'] = datetime.now().timestamp()
        return active_entries

    def _return_active_municipalities(self):
        # return a list of municipalities that are currently being listened for
        return [m for (m, t) in self._listeners if t > datetime.now().timestamp()]

    def _parse_montco_entry(self,i,s):
        p=[x.strip() for x in s.split(";") if x.strip()]
        t=p[-1].split("-Station")[0]
        st=(p[-1].split("Station")[-1].lstrip(":") if "Station" in p[-1]
            else p[2].split()[-1] if "Station" in p[2] else None)
        service,typ=i.split(":",1)
        return dict(
            service=service.strip(),
            type=re.sub(r'\W+$','',typ).strip(),
            intersection=p[0],
            municipality=p[1],
            station=st,
            time=int(datetime.strptime(t,"%Y-%m-%d @ %H:%M:%S")
                    .replace(tzinfo=ZoneInfo("America/New_York")).timestamp()))

if __name__ == "__main__":
    montco = MontCoDispatch()
