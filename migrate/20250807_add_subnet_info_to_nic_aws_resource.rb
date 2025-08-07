# frozen_string_literal: true

Sequel.migration do
  change do
    alter_table(:nic_aws_resource) do
      add_column :subnet_id, :text, unique: true
      add_column :subnet_az, :text
    end
  end
end
