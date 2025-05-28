# frozen_string_literal: true

class Clover
  def validate_postgres_input

    name = typecast_params.nonempty_str!("name")
    Validation.validate_postgres_name(name)

    flavor = typecast_params.nonempty_str("flavor", PostgresResource::Flavor::STANDARD)
    Validation.validate_postgres_flavor(flavor)
    subtree = option_tree["flavor"][flavor]

    available_locations = subtree["location"].keys.map(&:display_name)
    unless (subtree = subtree["location"].find { |k, v| k.id == @location.id }[1])
      fail ValidationFailed.new({location: "\"#{@location.display_name}\" is not a valid PostgreSQL location. Available locations: #{available_locations.join(", ")}"})
    end

    available_sizes = subtree["family"].values.flat_map { it["size"].keys }
    size = typecast_params.nonempty_str!("size")
    family = size.split("-").first
    unless (subtree = subtree["family"][family])
      fail ValidationFailed.new({size: "\"#{size}\" is not a valid PostgreSQL database size. Available sizes: #{available_sizes.join(", ")}"})
    end

    unless (subtree = subtree["size"][size])
      fail ValidationFailed.new({size: "\"#{size}\" is not a valid PostgreSQL database size. Available sizes: #{available_sizes.join(", ")}"})
    end

    available_storage_sizes = subtree["storage_size"].keys
    storage_size = typecast_params.nonempty_str("storage_size", subtree["storage_size"].keys.first)
    unless (subtree = subtree["storage_size"][storage_size])
      fail ValidationFailed.new({storage_size: "\"#{storage_size}\" is not a valid PostgreSQL storage size. Available storage sizes: #{available_storage_sizes.join(", ")}"})
    end

    available_ha_types = subtree["ha_type"].keys
    ha_type = typecast_params.nonempty_str("ha_type", PostgresResource::HaType::NONE)
    unless (subtree = subtree["ha_type"][ha_type])
      fail ValidationFailed.new({ha_type: "\"#{ha_type}\" is not a valid PostgreSQL HA type. Available HA types #{available_ha_types.join(", ")}"})
    end

    available_versions = option_tree["version"].keys
    version = typecast_params.nonempty_str("version", PostgresResource::DEFAULT_VERSION)
    unless available_versions.include?(version)
      fail ValidationFailed.new({version: "\"#{version}\" is not a valid PostgreSQL version. Available versions: #{available_versions.join(", ")}"})
    end

  end

  def postgres_post(name)
    authorize("Postgres:create", @project.id)
    fail Validation::ValidationFailed.new({billing_info: "Project doesn't have valid billing information"}) unless @project.has_valid_payment_method?

    Validation.validate_postgres_location(@location, @project.id)

    validate_postgres_input(flavor:, location: @location, size:, storage_size_gib:, version:, ha_type:)

    parsed_size = Validation.validate_postgres_size(@location, size, @project.id)

    requested_standby_count = case ha_type
    when PostgresResource::HaType::ASYNC then 1
    when PostgresResource::HaType::SYNC then 2
    else 0
    end

    requested_postgres_vcpu_count = (requested_standby_count + 1) * parsed_size.vcpu
    Validation.validate_vcpu_quota(@project, "PostgresVCpu", requested_postgres_vcpu_count)

    pg = nil
    DB.transaction do
      pg = Prog::Postgres::PostgresResourceNexus.assemble(
        project_id: @project.id,
        location_id: @location.id,
        name:,
        target_vm_size: parsed_size.vm_size,
        target_storage_size_gib: typecast_params.nonempty_str("storage_size") || parsed_size.storage_size_options.first,
        ha_type:,
        version: typecast_params.nonempty_str("version") || PostgresResource::DEFAULT_VERSION,
        flavor: typecast_params.nonempty_str("flavor") || PostgresResource::Flavor::STANDARD
      ).subject
      audit_log(pg, "create")
    end
    send_notification_mail_to_partners(pg, current_account.email)

    if api?
      Serializers::Postgres.serialize(pg, {detailed: true})
    else
      flash["notice"] = "'#{name}' will be ready in a few minutes"
      request.redirect "#{@project.path}#{pg.path}"
    end
  end

  def postgres_list
    dataset = dataset_authorize(@project.postgres_resources_dataset.eager, "Postgres:view").eager(:semaphores, :location, strand: :children)
    if api?
      dataset = dataset.where(location_id: @location.id) if @location
      paginated_result(dataset, Serializers::Postgres)
    else
      dataset = dataset.eager(:representative_server, :timeline)
      resources = dataset.all
        .group_by { |r| r.read_replica? ? r[:parent_id] : r[:id] }
        .flat_map { |group_id, rs| rs.sort_by { |r| r[:created_at] } }

      @postgres_databases = Serializers::Postgres.serialize(resources, {include_path: true})
      view "postgres/index"
    end
  end

  def send_notification_mail_to_partners(resource, user_email)
    if [PostgresResource::Flavor::PARADEDB, PostgresResource::Flavor::LANTERN].include?(resource.flavor) && (email = Config.send(:"postgres_#{resource.flavor}_notification_email"))
      flavor_name = resource.flavor.capitalize
      Util.send_email(email, "New #{flavor_name} Postgres database has been created.",
        greeting: "Hello #{flavor_name} team,",
        body: ["New #{flavor_name} Postgres database has been created.",
          "ID: #{resource.ubid}",
          "Location: #{resource.location.display_name}",
          "Name: #{resource.name}",
          "E-mail: #{user_email}",
          "Instance VM Size: #{resource.target_vm_size}",
          "Instance Storage Size: #{resource.target_storage_size_gib}",
          "HA: #{resource.ha_type}"])
    end
  end

  def generate_postgres_options(flavor: "standard", location: nil)
    options = OptionTreeGenerator.new

    options.add_option(name: "name")

    options.add_option(name: "flavor", values: flavor)

    options.add_option(name: "location", values: location || @project.postgres_locations, parent: "flavor", check: ->(flavor, location) {
      !(location.provider == "aws" && flavor != PostgresResource::Flavor::STANDARD)
    })

    options.add_option(name: "family", values: Option::POSTGRES_FAMILY_OPTIONS.map(&:name), parent: "location", check: ->(flavor, location, family) {
      location.provider != "aws" || family == "standard"
    })

    options.add_option(name: "size", values: Option::POSTGRES_SIZE_OPTIONS.map(&:name), parent: "family", check: ->(flavor, location, family, size) {
      family_from_size, vcpu_count = size.split("-")

      return false if family_from_size != family
      return false if location.provider == "aws" && vcpu_count.to_i > 16
      true
    })

    aws_storage_size_options = ["118", "237", "475", "950"]
    storage_size_options = Option::POSTGRES_STORAGE_SIZE_OPTIONS + aws_storage_size_options
    options.add_option(name: "storage_size", values: storage_size_options, parent: "size", check: ->(flavor, location, family, size, storage_size) {
      vcpu_count = size.split("-").last.to_i

      if location.provider == "aws"
        storage_index = Math.log2(vcpu_count).ceil - 1
        aws_storage_size_options[storage_index] == storage_size
      else
        min_storage = (vcpu_count >= 30) ? 1024 : vcpu_count * 32
        min_storage /= 2 if family == "burstable"
        [min_storage, min_storage * 2, min_storage * 4].include?(storage_size.to_i)
      end
    })

    options.add_option(name: "version", values: Option::POSTGRES_VERSION_OPTIONS)

    options.add_option(name: "ha_type", values: Option::POSTGRES_HA_OPTIONS.map(&:name), parent: "storage_size")

    options.serialize
  end
end
