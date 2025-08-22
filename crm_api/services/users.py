import csv
import io
import secrets
import string
from typing import List, Dict
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission, Group
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from crm_api.models import Actives


def _gen_password() -> str:

    digits = "".join(secrets.choice(string.digits) for _ in range(4))
    letters = "".join(secrets.choice(string.ascii_lowercase) for _ in range(2))
    return f"Op-{digits}{letters}"


def _ensure_change_permission_for_actives() -> Permission:
    ct = ContentType.objects.get_for_model(Actives)
    perm, _ = Permission.objects.get_or_create(
        codename=f"change_{Actives._meta.model_name}",
        content_type=ct,
        defaults={"name": "Can change Abonent"},
    )
    return perm


@transaction.atomic
def bulk_create_operators(*, count: int = 50, prefix: str = "operator",
                          start: int = 0, reset_existing: bool = False) -> List[Dict[str, str]]:
    User = get_user_model()
    results: List[Dict[str, str]] = []

    perm_change_actives = _ensure_change_permission_for_actives()

    grp, _ = Group.objects.get_or_create(name="Operators")
    grp.permissions.add(perm_change_actives)

    for i in range(start, start + count):
        username = prefix if i == 0 else f"{prefix}{i}"
        password = _gen_password()

        user, created = User.objects.get_or_create(
            username=username,
            defaults={
                "role": getattr(User, "ROLE_OPERATOR", "operator"),
                "is_staff": False,
                "is_superuser": False,
                "fio": f"Operator #{i}",
            },
        )

        reset = False
        if created:
            user.set_password(password)
            user.save(update_fields=["password", "role", "is_staff", "is_superuser", "fio"])
        else:
            if reset_existing:
                user.set_password(password)
                if not user.fio:
                    user.fio = f"Operator #{i}"
                user.save(update_fields=["password", "fio"])
                reset = True
            else:
                password = ""

        user.user_permissions.add(perm_change_actives)
        user.groups.add(grp)

        results.append(
            {"username": username, "password": password, "created": "1" if created else "0", "reset": "1" if reset else "0"}
        )

    return results


def build_csv_from_results(rows: List[Dict[str, str]]) -> io.StringIO:

    buff = io.StringIO()
    w = csv.DictWriter(buff, fieldnames=["username", "password", "created", "reset"])
    w.writeheader()
    w.writerows(rows)
    buff.seek(0)
    return buff
