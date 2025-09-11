import csv
import io
import secrets
import string
from typing import List, Dict

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission, Group
from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from crm_api.models import *


def _gen_password() -> str:
    digits = "".join(secrets.choice(string.digits) for _ in range(4))
    letters = "".join(secrets.choice(string.ascii_lowercase) for _ in range(2))
    return f"Op-{digits}{letters}"


def _ensure_change_permission_for_fixeds() -> Permission:
    ct = ContentType.objects.get_for_model(Fixeds)
    perm, _ = Permission.objects.get_or_create(
        codename=f"change_{Fixeds._meta.model_name}",
        content_type=ct,
        defaults={"name": "Can change Fixeds"},
    )
    return perm


def _ensure_change_permission_for_suspends() -> Permission:
    ct = ContentType.objects.get_for_model(Suspends, for_concrete_model=False)
    perm, _ = Permission.objects.get_or_create(
        codename=f"change_{Suspends._meta.model_name}",
        content_type=ct,
        defaults={"name": "Can change Suspends"},
    )
    return perm


def _ensure_change_permission_for_Actives() -> Permission:
    ct = ContentType.objects.get_for_model(Actives, for_concrete_model=False)
    perm, _ = Permission.objects.get_or_create(
        codename=f"change_{Actives._meta.model_name}",
        content_type=ct,
        defaults={"name": "Can change Actives"},
    )
    return perm

@transaction.atomic
def bulk_create_operators(
    *,
    count: int = 50,
    prefix: str = "operator",
    start: int = 0,
    reset_existing: bool = False
) -> List[Dict[str, str]]:
    User = get_user_model()
    results: List[Dict[str, str]] = []

    perm_change_fixeds = _ensure_change_permission_for_fixeds()
    perm_change_suspends = _ensure_change_permission_for_suspends()
    perm_change_actives = _ensure_change_permission_for_Actives()

    grp, _ = Group.objects.get_or_create(name="Operators")
    grp.permissions.add(perm_change_fixeds, perm_change_suspends,perm_change_actives)

    for i in range(start, start + count):
        username = prefix if i == 0 else f"{prefix}{i}"
        password = _gen_password()


        first_name = f"#{i}"
        last_name = "Operator"

        user, created = User.objects.get_or_create(
            username=username,
            defaults={
                "role": getattr(User, "ROLE_OPERATOR", "operator"),
                "is_staff": False,
                "is_superuser": False,
                "first_name": first_name,
                "last_name": last_name,
            },
        )

        reset = False
        if created:
            user.set_password(password)
            user.save(update_fields=["password", "role", "is_staff", "is_superuser", "first_name", "last_name"])
        else:
            if reset_existing:
                user.set_password(password)
                if not user.first_name:
                    user.first_name = first_name
                if not user.last_name:
                    user.last_name = last_name
                user.save(update_fields=["password", "first_name", "last_name"])
                reset = True
            else:
                password = ""

        user.user_permissions.add(perm_change_fixeds, perm_change_suspends,perm_change_actives)
        user.groups.add(grp)

        results.append(
            {
                "username": username,
                "password": password,
                "created": "1" if created else "0",
                "reset": "1" if reset else "0",
            }
        )

    return results


def build_csv_from_results(rows: List[Dict[str, str]]) -> io.StringIO:
    buff = io.StringIO()
    w = csv.DictWriter(buff, fieldnames=["username", "password", "created", "reset"])
    w.writeheader()
    w.writerows(rows)
    buff.seek(0)
    return buff
