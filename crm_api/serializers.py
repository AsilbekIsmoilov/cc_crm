from rest_framework import serializers
from .models import *
from django.utils import timezone


class ActivesFixationWriteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Actives
        fields = ["status_call", "call_result", "abonent_answer", "note", "tech"]


class ActivesSerializer(serializers.ModelSerializer):
    called_by_id = serializers.IntegerField(source="fixed_by_id", read_only=True)
    called_by = serializers.CharField(source="who_called", read_only=True)
    called_at = serializers.DateTimeField(source="fixed_at", read_only=True)

    class Meta:
        model = Actives
        fields = [
            "id",
            "msisdn","departments","status_from","days_in_status","write_offs_date",
            "client","rate_plan","balance","subscription_fee","account",
            "branches","status","phone",
            "status_call","call_result","abonent_answer","note","tech",
            "called_by_id","called_by","called_at",
            "created_at","updated_at",
        ]
        read_only_fields = ["called_by_id","called_by","called_at","created_at","updated_at"]


class ExcelUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExcelUpload
        fields = (
            "id", "file", "original_name",
            "uploaded_at"
        )

    def create(self, validated_data):
        user = self.context["request"].user
        if not validated_data.get("original_name"):
            f = validated_data.get("file")
            if f and getattr(f, "name", None):
                validated_data["original_name"] = f.name
        return super().create(validated_data)


class ImportResultSerializer(serializers.Serializer):
    files = ExcelUploadSerializer(many=True)
    imported_rows = serializers.IntegerField()
    processed_files = serializers.IntegerField()
    started_sbms_sync = serializers.BooleanField()



class OperatorSerializer(serializers.ModelSerializer):
    display = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ("id", "username", "fio", "display")

    def get_display(self, obj):
        return (obj.fio or obj.get_full_name() or obj.username)



class MeSerializer(serializers.ModelSerializer):
    display = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id", "username","is_staff", "is_superuser", "display",
        ]

    def get_display(self, obj: User) -> str:
        full = (getattr(obj, "fio", None) or obj.get_full_name() or "").strip()
        return full or obj.username


class UploadJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = UploadJob
        fields = [
            "id", "status", "total_rows", "processed_rows",
            "succeeded_rows", "failed_rows", "last_error", "created_at",
        ]


class FixedsSerializer(serializers.ModelSerializer):
    fixed_by_label = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = Fixeds
        fields = [
            "id",
            "msisdn","departments","status_from","days_in_status","write_offs_date",
            "client","rate_plan","balance","subscription_fee","account","branches",
            "status","phone",
            "status_call","call_result","abonent_answer","note","tech",
            "fixed_by","fixed_by_label","fixed_at",
            "created_at","updated_at","moved_at",
        ]
        read_only_fields = ["id", "moved_at", "fixed_by_label"]

    def get_fixed_by_label(self, obj):
        u = obj.fixed_by
        if not u:
            return ""
        return getattr(u, "fio", None) or u.get_full_name() or u.username

    def validate(self, attrs):
        msisdn = attrs.get("msisdn", getattr(self.instance, "msisdn", None))
        fixed_at = attrs.get("fixed_at", getattr(self.instance, "fixed_at", None))
        if msisdn and fixed_at:
            qs = Fixeds.objects.filter(msisdn=msisdn, fixed_at=fixed_at)
            if self.instance:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise serializers.ValidationError("Запись с таким msisdn и fixed_at уже существует.")
        return attrs

    def create(self, validated_data):
        validated_data.setdefault("created_at", timezone.now())
        validated_data.setdefault("updated_at", validated_data["created_at"])
        return super().create(validated_data)

    def update(self, instance, validated_data):
        validated_data["updated_at"] = timezone.now()
        return super().update(instance, validated_data)