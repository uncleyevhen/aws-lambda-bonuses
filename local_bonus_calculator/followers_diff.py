"""Отримання невзаємних підписок безпосередньо з Instagram."""
from __future__ import annotations

import argparse
import json
import os
import sys
from getpass import getpass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from instagrapi import Client
from instagrapi.exceptions import ChallengeRequired, ClientError, LoginRequired


def _normalize_items(items: Iterable[str], *, case_sensitive: bool) -> Tuple[Set[str], Dict[str, str]]:
    lookup: Dict[str, str] = {}
    normalized_set: Set[str] = set()
    for item in items:
        normalized = item if case_sensitive else item.lower()
        normalized_set.add(normalized)
        lookup.setdefault(normalized, item)
    return normalized_set, lookup


def _format_section(title: str, values: Sequence[str]) -> str:
    if not values:
        return f"{title}: нема"
    bullet_list = "\n".join(f"  - {value}" for value in sorted(values, key=str.casefold))
    return f"{title}:\n{bullet_list}"


def _resolve_password(password: Optional[str]) -> str:
    if password:
        return password
    env_password = os.getenv("INSTAGRAM_PASSWORD")
    if env_password:
        return env_password
    try:
        return getpass("Введіть пароль Instagram: ")
    except (EOFError, KeyboardInterrupt) as exc:  # pragma: no cover - interactive fallback
        raise SystemExit("Пароль Instagram необхідно передати через --password або змінну оточення.") from exc


def _load_snapshot(path: Optional[Path]) -> List[str]:
    if not path:
        return []
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Не вдалося прочитати попередній знімок з {path}: {exc}") from exc
    if not isinstance(data, list):
        raise ValueError(f"Формат файлу {path} не підтримується. Очікується список рядків.")
    return [str(item) for item in data]


def _save_snapshot(path: Optional[Path], followers: Sequence[str]) -> None:
    if not path:
        return
    path.write_text(json.dumps(sorted(followers, key=str.casefold), ensure_ascii=False, indent=2), encoding="utf-8")


def _load_client(session_file: Optional[Path]) -> Client:
    client = Client()
    if session_file and session_file.exists():
        try:
            settings = json.loads(session_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            settings = None
        if settings:
            client.set_settings(settings)
    return client


def _persist_session(client: Client, session_file: Optional[Path]) -> None:
    if not session_file:
        return
    session_file.write_text(json.dumps(client.get_settings(), ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_user_lists(client: Client, username: str) -> Tuple[List[str], List[str]]:
    user_id = client.user_id_from_username(username)
    followers = [user.username for user in client.user_followers(user_id).values()]
    following = [user.username for user in client.user_following(user_id).values()]
    return followers, following


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Авторизація в Instagram та пошук невзаємних підписок і відписок безпосередньо через API."
        )
    )
    parser.add_argument("--username", required=True, help="Ваш логін Instagram.")
    parser.add_argument("--password", help="Пароль Instagram (можна передати через змінну INSTAGRAM_PASSWORD).")
    parser.add_argument(
        "--target",
        help="Користувач, для якого знімати підписки/підписників (за замовчуванням той самий, що --username).",
    )
    parser.add_argument(
        "--session-file",
        type=Path,
        help="JSON файл зессії instagrapi для збереження/повторного використання авторизації.",
    )
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        help="JSON файл з попереднім списком підписників для відстеження відписок.",
    )
    parser.add_argument(
        "--no-update-snapshot",
        action="store_true",
        help="Не оновлювати snapshot-файл після виконання скрипту.",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Не ігнорувати регістр при порівнянні ніків.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    password = _resolve_password(args.password)
    client = _load_client(args.session_file)

    try:
        client.login(args.username, password)
    except ChallengeRequired as exc:
        parser.error(
            "Instagram вимагає додаткове підтвердження (Challenge Required). Увійдіть вручну в офіційному "
            "додатку, виконайте підтвердження й повторіть спробу."
        )
        return 1
    except ClientError as exc:
        parser.error(f"Не вдалося авторизуватись: {exc}")
        return 1

    _persist_session(client, args.session_file)

    target_username = args.target or args.username
    try:
        followers, following = _fetch_user_lists(client, target_username)
    except LoginRequired:
        parser.error("Сесія недійсна. Спробуйте знову з параметрами --username та --password.")
        return 1
    except ClientError as exc:
        parser.error(f"Не вдалося отримати дані користувача {target_username}: {exc}")
        return 1

    followers_set, followers_lookup = _normalize_items(followers, case_sensitive=args.case_sensitive)
    following_set, following_lookup = _normalize_items(following, case_sensitive=args.case_sensitive)

    non_mutual_keys = following_set - followers_set
    non_mutual = [following_lookup[key] for key in non_mutual_keys]

    output_sections = [_format_section("Невзаємні підписки", non_mutual)]

    previous_followers = _load_snapshot(args.snapshot_file)
    if previous_followers:
        previous_set, previous_lookup = _normalize_items(
            previous_followers, case_sensitive=args.case_sensitive
        )
        unfollowed_keys = previous_set - followers_set
        unfollowed = [previous_lookup[key] for key in unfollowed_keys]
        output_sections.append(_format_section("Відписалися", unfollowed))

        new_followers_keys = followers_set - previous_set
        if new_followers_keys:
            new_followers = [followers_lookup[key] for key in new_followers_keys]
            output_sections.append(_format_section("Нові підписники", new_followers))

    print("\n\n".join(output_sections))

    if args.snapshot_file and not args.no_update_snapshot:
        _save_snapshot(args.snapshot_file, followers)

    return 0


if __name__ == "__main__":
    sys.exit(main())
