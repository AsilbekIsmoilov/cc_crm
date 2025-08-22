from rest_framework import serializers
from .models import *

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


class SBMSAccountSerializer(serializers.ModelSerializer):
    google_accounts_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = SBMSAccount
        fields = ("id", "label", "username", "is_active", "max_google_accounts", "google_accounts_count")

class GoogleAccountSerializer(serializers.ModelSerializer):
    sbms_account = SBMSAccountSerializer(read_only=True)
    sbms_account_id = serializers.PrimaryKeyRelatedField(
        queryset=SBMSAccount.objects.filter(is_active=True),
        source="sbms_account", write_only=True
    )

    class Meta:
        model = GoogleAccount
        fields = (
            "id", "label", "google_email", "user_data_dir", "profile_directory", "chrome_binary",
            "is_active", "sbms_account", "sbms_account_id"
        )

class ExcelUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExcelUpload
        fields = (
            "id", "file", "original_name",
            "status", "rows_found", "rows_saved", "uploaded_by", "uploaded_at", "processed_at", "log", "batch_tag"
        )
        read_only_fields = ("status", "rows_found", "rows_saved", "uploaded_by", "uploaded_at", "processed_at", "log")

    def create(self, validated_data):
        user = self.context["request"].user
        validated_data["uploaded_by"] = user
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