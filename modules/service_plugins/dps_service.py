#!/usr/bin/env python3
"""
Montgomery County DPS CAD feed relay service for MeshCore Bot
"""

import asyncio
from shutil import copy
from ..montco_helper import MontCoDispatch
from .base_service import BaseServicePlugin
from typing import Any, Optional
from datetime import datetime, timezone, MINYEAR
import copy

METADATA_KEY_LAST_POSTED_TIME = "dps_last_posted_time"

from meshcore import EventType

class MontCoPublicSafetyService(BaseServicePlugin):
    """Montgomery County DPS CAD feed relay service for MeshCore Bot."""

    config_section = "MontCo_DPS_Service"
    description = "Relays Montgomery County DPS CAD feed to MeshCore channels"

    # Sample municipalities set
    municipalities = {'Abington', 'Ambler', 'Bridgeport', 'Bryn Athyn', 'Cheltenham', 
        'Conshohocken', 'Douglass', 'East Greenville', 'East Norriton', 
        'Franconia', 'Green Lane', 'Hatboro', 'Hatfield Township', 'Horsham', 
        'Jenkintown', 'Lansdale', 'Limerick', 'Lower Frederick', 
        'Lower Gwynedd', 'Lower Merion', 'Lower Moreland', 
        'Lower Pottsgrove', 'Lower Providence', 'Lower Salford', 
        'Marlborough', 'Montgomery', 'Narberth', 'New Hanover', 
        'Norristown', 'North Wales', 'Pennsburg', 'Perkiomen', 
        'Plymouth', 'Pottstown', 'Red Hill', 'Rockledge', 
        'Royersford', 'Salford', 'Schwenksville', 'Skippack', 
        'Souderton', 'Springfield', 'Telford', 'Towamencin', 
        'Trappe', 'Upper Dublin', 'Upper Frederick', 'Upper Gwynedd', 
        'Upper Hanover', 'Upper Merion', 'Upper Moreland', 
        'Upper Pottsgrove', 'Upper Providence', 'Upper Salford', 
        'West Conshohocken', 'West Norriton', 'West Pottsgrove', 
        'Whitemarsh', 'Whitpain', 'Worcester'}

    settings_schema = [
        {
            "key": "channel",
            "label": "Channel",
            "type": "str",
            "default": "montco",
            "help": "Channel name to post alerts to (e.g. #alerts).",
            "required": True,
        },
        {
            "key": "poll_interval",
            "label": "Poll interval",
            "type": "int",
            "min": 1000,
            "default": 300000,
            "unit": "ms",
            "help": "How often to poll, in milliseconds.",
        },
        {
            "key": "time_window_minutes",
            "label": "Time window",
            "type": "int",
            "min": 1,
            "default": 10,
            "unit": "min",
            "help": "Look-back window for new events, in minutes.",
        }
    ]


    def __init__(self, bot: Any) -> None:
        super().__init__(bot)
        self.enabled = True
        self.logger.info("MontCoPublicSafetyService initialization started")

        self._running = False
        self._subscriptions = {}
        self._municipality_lookup = {m.upper(): m for m in self.municipalities}
        self.logger.info("MontCoPublicSafetyService initialization complete")

    def _normalize_municipality_name(self, municipality: str) -> str:
        """Return the canonical municipality name for a user-entered value."""
        cleaned = (municipality or "").strip()
        if not cleaned:
            return ""
        return self._municipality_lookup.get(cleaned.upper(), "")

    async def start(self) -> None:
        if not self.enabled:
            self.logger.info("MontCoPublicSafetyService is disabled")
            return

        self.logger.info("Starting MontCoPublicSafetyService...")
        self.bot.meshcore.subscribe(EventType.CHANNEL_MSG_RECV, self._on_mesh_channel_message)
        self.logger.info("Subscribed to CHANNEL_MSG_RECV events")

        self._running = True
        self._montco = MontCoDispatch()
        await self._montco.refresh()

        section = "MontCo_DPS_Service"
        self.channel = self.bot.config.get(section, "channel", fallback="#montco")
        poll_ms = self.bot.config.getint(section, "poll_interval", fallback=60000)
        self.poll_interval_seconds = poll_ms / 1000.00
        self.time_window_minutes = self.bot.config.getint(
            section, "time_window_minutes", fallback=10
        )
        self.logger.info("Entries found in feed: %d", len(self._montco._entries))

        await self._send_startup_message()
        # Start the feed relay loop
        # self._send_startup_message()
        asyncio.create_task(self._poll_loop())
        self.logger.info("MontCoPublicSafetyService started")

    async def stop(self) -> None:
        self.logger.info("Stopping MontCoPublicSafetyService...")
        self._running = False
        # self.bot.meshcore.unsubscribe(EventType.CHANNEL_MSG_RECV, self._on_mesh_channel_message)
        self.logger.info("MontCoPublicSafetyService stopped")

    async def _on_mesh_channel_message(self, event, metadata=None) -> None:
        try:
            payload = copy.deepcopy(event.payload) if hasattr(event, 'payload') else None
            if payload is None:
                self.logger.warning("Channel message event has no payload")
                return

            channel_idx = payload.get('channel_idx', 0)
            channel_name = self.bot.channel_manager.get_channel_name(channel_idx)
            if channel_name != self.channel:
                self.logger.debug("Ignoring message from channel %s (not %s)", channel_name, self.channel)
                return

            text = payload.get('text', '')
            sender = 'Unknown'
            command = ''

            if ':' in text and not text.startswith('http'):
                parts = text.split(':', 1)
                sender = parts[0].strip()
                command = parts[1].strip()

            # Command should be in the format of "dps [hours] [municipality],[(opt)municipality]"
            # Hours are integers, municipalities are strings, and municipalities can have spaces in their names
            # and must be in the list of municipalities defined in the service.
            if command:
                command = command.strip()
                if command.lower().startswith("dps"):
                    command = command[3:].strip()
                if not command:
                    self.logger.warning("Invalid command format: %s", command)
                else:
                    parts = command.split(None, 1)
                    if len(parts) == 2:
                        try:
                            hours = int(parts[0])
                            if hours > 4:
                                self.logger.warning("Ignoring command with hours > 4: %s", command)
                                await self.bot.command_manager.send_channel_message(
                                    self.channel,
                                    f"Cannot add listener for more than 4 hours. Ignoring command: {command}",
                                    scope=self.get_mesh_flood_scope()
                                )
                                return
                            raw_municipalities = [m.strip() for m in parts[1].split(',') if m.strip()]
                            if not raw_municipalities:
                                self.logger.warning("Invalid command format: %s", command)
                                return

                            for raw_municipality in raw_municipalities:
                                municipality = self._normalize_municipality_name(raw_municipality)
                                if not municipality:
                                    self.logger.warning("Ignoring unknown municipality: %s", raw_municipality)
                                    continue
                                self._montco.add_listener(municipality.upper(), hours * 3600)
                                self.logger.info("Added listener for %s for %d hours", municipality, hours)
                                await self.bot.command_manager.send_channel_message(
                                    self.channel,
                                    f"Added listener for {municipality} for {hours} hours",
                                    scope=self.get_mesh_flood_scope()
                                )
                        except ValueError:
                            self.logger.warning("Invalid command format: %s", command)
                    else:
                        self.logger.warning("Invalid command format: %s", command)

            if not channel_name or channel_name.lower() in ('dm', 'direct', 'private'):
                self.logger.debug("Ignoring DM (DMs are never bridged)")
                return

        except Exception as e:
            self.logger.error(f"Error handling mesh channel message: {e}", exc_info=True)

    async def _send_startup_message(self):
        try:
            await self.bot.command_manager.send_channel_message(self.channel,
                "DPS Bot Listening for commands", scope=self.get_mesh_flood_scope())
        except Exception as e:
            self.logger.error("Error sending startup message", e)

    async def _poll_loop(self) -> None:
        self.logger.info(
            "Dispatch poll loop started (interval=%.1fs, window=%d min)",
            self.poll_interval_seconds,
            self.time_window_minutes,
        )
        while self._running:
            try:
                await self._check_dispatches()
                await asyncio.sleep(self.poll_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in dispatch poll loop: %s", e)
                await asyncio.sleep(60)

    async def _check_dispatches(self) -> None:
        # Refresh the feed
        await self._montco.refresh()
        # Get active messages
        messages = self._montco.get_messages()
        self.logger.info("Found %d new dispatch messages", len(messages))
        for message in messages:
            message = message.lower()
            self.logger.info("Posting message: %s", message)
            await self.bot.command_manager.send_channel_message(
                self.channel, message, scope=self.get_mesh_flood_scope())
            await asyncio.sleep(10)

    def _load_last_posted_time_ms(self) -> int:
        """Load last posted event time (ms) from bot_metadata to avoid reposts after restart."""
        if not getattr(self.bot, "db_manager", None):
            return 0
        raw = self.bot.db_manager.get_metadata(METADATA_KEY_LAST_POSTED_TIME)
        if not raw:
            return 0
        try:
            return int(raw)
        except ValueError:
            return 0
